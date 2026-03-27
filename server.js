const express = require('express');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const https = require('https');
const multer = require('multer');
const bcrypt = require('bcryptjs');
const nodemailer = require('nodemailer');
const crypto = require('crypto');
const { Server } = require('socket.io');

require('dotenv').config();

// Базовая конфигурация сервера и пути к статике/данным.
const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 3000);
const SSL_KEY_PATH = process.env.SSL_KEY_PATH || '';
const SSL_CERT_PATH = process.env.SSL_CERT_PATH || '';
const USE_HTTPS = Boolean(SSL_KEY_PATH && SSL_CERT_PATH);

const PUBLIC_DIR = path.join(__dirname, 'public');
const UPLOADS_DIR = path.join(__dirname, 'uploads');
const DATA_DIR = path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const CHATS_FILE = path.join(DATA_DIR, 'chats.json');

const PASSWORD_MIN_LENGTH = 8;
const PASSWORD_MAX_LENGTH = 72;
const PASSWORD_RESET_TTL_MS = 15 * 60 * 1000;
const PASSWORD_RESET_CODE_LENGTH = 6;
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const ALLOWED_REACTIONS = new Set(['👍', '❤️', '😂', '🔥', '😍', '😮', '😢', '😡', '🎉']);

ensureDirectoryExists(UPLOADS_DIR);
ensureDirectoryExists(DATA_DIR);

const app = express();
const server = createHttpServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
  },
  transports: ['websocket', 'polling'],
  maxHttpBufferSize: 10 * 1024 * 1024,
  pingInterval: 25000,
  pingTimeout: 60000,
  connectTimeout: 45000,
});

// Все основные индексы хранятся в памяти, а на диск сбрасываются отдельным слоем ниже.
const registeredAccountsById = new Map();
const registeredAccountIdByUsername = new Map();
const registeredAccountIdByEmail = new Map();

const onlineSessionsBySocketId = new Map();
const socketIdByAccountId = new Map();
const sessionByToken = new Map();
const sessionTokenBySocketId = new Map();

const chatsById = new Map();
const messagesByChatId = new Map();
const callsByChatId = new Map();
const passwordResetRequestsByCode = new Map();

let mailTransportPromise = null;
let chatSaveTimer = null;
const CHAT_SAVE_DEBOUNCE_MS = 500;

// Поднимаем состояние из файлов до старта HTTP и сокет-обработчиков.
loadRegisteredAccountsFromDisk();
loadChatsFromDisk();
ensureDefaultChannels();

// Multer сохраняет загруженные вложения и аватары в локальную папку uploads.
const uploadStorage = multer.diskStorage({
  destination: function getDestination(req, file, callback) {
    callback(null, UPLOADS_DIR);
  },
  filename: function getFilename(req, file, callback) {
    const safeOriginalName = String(file.originalname || 'file')
      .replace(/\s+/g, '_')
      .replace(/[^a-zA-Z0-9._-]/g, '');
    const uniquePrefix = `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
    callback(null, `${uniquePrefix}-${safeOriginalName}`);
  },
});

const upload = multer({
  storage: uploadStorage,
  limits: {
    fileSize: 80 * 1024 * 1024,
  },
});

app.disable('x-powered-by');
app.set('trust proxy', 1);
app.use(express.json());
app.use('/uploads', express.static(UPLOADS_DIR));
app.use(express.static(PUBLIC_DIR));

app.get('/api/health', handleHealthCheck);
app.post('/api/upload', upload.single('file'), handleUpload);
app.post('/api/auth/request-password-reset', handleRequestPasswordReset);
app.post('/api/auth/reset-password', handleResetPassword);
app.get('*', handleSpaFallback);

// Один вход сокета маршрутизирует все realtime-сценарии приложения.
io.on('connection', handleSocketConnection);

server.listen(PORT, HOST, handleServerStart);

function handleServerStart() {
  const protocol = USE_HTTPS ? 'https' : 'http';
  const addresses = getServerAddresses(HOST, PORT, protocol);

  console.log(`Hexlet Messenger server is running at:`);
  addresses.forEach(function printAddress(address) {
    console.log(`- ${address}`);
  });

  if (!USE_HTTPS) {
    console.log('Calls need HTTPS or localhost secure context to access microphone/camera.');
  }
}

function handleHealthCheck(req, res) {
  cleanupExpiredPasswordResetRequests();
  cleanupExpiredSessions();

  res.json({
    status: 'ok',
    protocol: USE_HTTPS ? 'https' : 'http',
    uptimeSeconds: Math.floor(process.uptime()),
    onlineUsers: onlineSessionsBySocketId.size,
    activeSessions: sessionByToken.size,
    registeredAccounts: registeredAccountsById.size,
    now: new Date().toISOString(),
  });
}

function handleUpload(req, res) {
  if (!req.file) {
    res.status(400).json({ ok: false, error: 'File was not provided.' });
    return;
  }

  const file = req.file;
  const fileUrl = `/uploads/${file.filename}`;

  res.json({
    ok: true,
    file: {
      url: fileUrl,
      name: file.originalname,
      mimeType: file.mimetype,
      size: file.size,
      kind: detectAttachmentKind(file.mimetype),
    },
  });
}

async function handleRequestPasswordReset(req, res) {
  try {
    cleanupExpiredPasswordResetRequests();

    const rawEmail = req.body && req.body.email;
    const rawUsername = req.body && req.body.username;
    const email = normalizeEmail(rawEmail);
    const username = normalizeUsername(rawUsername);

    if (!isValidEmail(email)) {
      res.status(400).json({
        ok: false,
        error: 'Укажи корректный email для восстановления.',
      });
      return;
    }

    let account = findAccountByEmail(email);

    if (account && username && account.usernameLower !== username.toLowerCase()) {
      account = null;
    }

    if (!account) {
      res.json({
        ok: true,
        message: 'Если аккаунт с такими данными существует, код отправлен на почту.',
      });
      return;
    }

    const code = createUniqueResetCode();
    const nowIso = new Date().toISOString();

    removeResetCodesForAccount(account.id);

    passwordResetRequestsByCode.set(code, {
      code,
      accountId: account.id,
      email: account.email,
      createdAt: nowIso,
      expiresAt: Date.now() + PASSWORD_RESET_TTL_MS,
    });

    const resetLink = buildPasswordResetLink(req, account.email, code);

    await sendPasswordResetEmail({
      toEmail: account.email,
      username: account.username,
      code,
      resetLink,
    });

    const responsePayload = {
      ok: true,
      message: 'Письмо для восстановления отправлено на указанный email.',
      expiresInMinutes: Math.floor(PASSWORD_RESET_TTL_MS / 60000),
    };

    res.json(responsePayload);
  } catch (error) {
    console.error('Password reset request error:', error);
    res.status(500).json({
      ok: false,
      error: 'Не удалось отправить код восстановления. Проверь настройки email-сервера.',
    });
  }
}

function handleResetPassword(req, res) {
  cleanupExpiredPasswordResetRequests();

  const email = normalizeEmail(req.body && req.body.email);
  const code = normalizeResetCode(req.body && req.body.code);
  const newPassword = normalizePassword(req.body && req.body.newPassword);

  if (!isValidEmail(email)) {
    res.status(400).json({ ok: false, error: 'Некорректный email.' });
    return;
  }

  if (!isValidResetCode(code)) {
    res.status(400).json({ ok: false, error: 'Код восстановления должен состоять из 6 цифр.' });
    return;
  }

  const passwordValidationError = validatePassword(newPassword);

  if (passwordValidationError) {
    res.status(400).json({ ok: false, error: passwordValidationError });
    return;
  }

  const resetRequest = passwordResetRequestsByCode.get(code);

  if (!resetRequest) {
    res.status(400).json({ ok: false, error: 'Код восстановления неверный или устарел.' });
    return;
  }

  if (resetRequest.expiresAt <= Date.now()) {
    passwordResetRequestsByCode.delete(code);
    res.status(400).json({ ok: false, error: 'Срок действия кода истёк. Запроси новый код.' });
    return;
  }

  if (resetRequest.email !== email) {
    res.status(400).json({ ok: false, error: 'Email не совпадает с запросом восстановления.' });
    return;
  }

  const account = registeredAccountsById.get(resetRequest.accountId);

  if (!account) {
    passwordResetRequestsByCode.delete(code);
    res.status(400).json({ ok: false, error: 'Аккаунт не найден.' });
    return;
  }

  account.passwordHash = bcrypt.hashSync(newPassword, 12);
  account.updatedAt = new Date().toISOString();

  saveAccountsToDisk();
  removeResetCodesForAccount(account.id);

  res.json({ ok: true, message: 'Пароль успешно изменён. Теперь можно войти с новым паролем.' });
}

function handleSpaFallback(req, res) {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
}

// Здесь только связываем имя события с отдельным обработчиком, чтобы доменная логика жила ниже по секциям.
function handleSocketConnection(socket) {
  socket.on('auth:register', function onAuthRegister(payload, ack) {
    handleAuthRegister(socket, payload, ack);
  });

  socket.on('auth:login', function onAuthLogin(payload, ack) {
    handleAuthLogin(socket, payload, ack);
  });

  socket.on('auth:resume', function onAuthResume(payload, ack) {
    handleAuthResume(socket, payload, ack);
  });

  socket.on('auth:logout', function onAuthLogout(_, ack) {
    handleAuthLogout(socket, ack);
  });

  socket.on('auth:updateSettings', function onAuthUpdateSettings(payload, ack) {
    handleAuthUpdateSettings(socket, payload, ack);
  });

  socket.on('user:search', function onUserSearch(payload, ack) {
    handleUserSearch(socket, payload, ack);
  });

  socket.on('chat:createPrivate', function onCreatePrivate(payload, ack) {
    handleCreatePrivateChat(socket, payload, ack);
  });

  socket.on('chat:createGroup', function onCreateGroup(payload, ack) {
    handleCreateGroupChat(socket, payload, ack);
  });

  socket.on('chat:createChannel', function onCreateChannel(payload, ack) {
    handleCreateChannel(socket, payload, ack);
  });

  socket.on('channel:search', function onChannelSearch(payload, ack) {
    handleChannelSearch(socket, payload, ack);
  });

  socket.on('channel:subscribe', function onChannelSubscribe(payload, ack) {
    handleChannelSubscribe(socket, payload, ack);
  });

  socket.on('chat:list', function onChatList(_, ack) {
    handleChatList(socket, ack);
  });

  socket.on('chat:members', function onChatMembers(payload, ack) {
    handleChatMembers(socket, payload, ack);
  });

  socket.on('chat:memberRemove', function onChatMemberRemove(payload, ack) {
    handleChatMemberRemove(socket, payload, ack);
  });

  socket.on('chat:adminAdd', function onChatAdminAdd(payload, ack) {
    handleChatAdminAdd(socket, payload, ack);
  });

  socket.on('chat:adminRemove', function onChatAdminRemove(payload, ack) {
    handleChatAdminRemove(socket, payload, ack);
  });

  socket.on('message:history', function onMessageHistory(payload, ack) {
    handleMessageHistory(socket, payload, ack);
  });

  socket.on('message:send', function onMessageSend(payload, ack) {
    handleMessageSend(socket, payload, ack);
  });

  socket.on('message:reaction', function onMessageReaction(payload, ack) {
    handleMessageReaction(socket, payload, ack);
  });

  socket.on('chat:typing', function onChatTyping(payload) {
    handleTyping(socket, payload);
  });

  socket.on('call:start', function onCallStart(payload, ack) {
    handleCallStart(socket, payload, ack);
  });

  socket.on('call:join', function onCallJoin(payload, ack) {
    handleCallJoin(socket, payload, ack);
  });

  socket.on('call:leave', function onCallLeave(payload, ack) {
    handleCallLeave(socket, payload, ack);
  });

  socket.on('webrtc:offer', function onOffer(payload, ack) {
    handleWebRtcOffer(socket, payload, ack);
  });

  socket.on('webrtc:answer', function onAnswer(payload, ack) {
    handleWebRtcAnswer(socket, payload, ack);
  });

  socket.on('webrtc:ice-candidate', function onIceCandidate(payload, ack) {
    handleWebRtcIceCandidate(socket, payload, ack);
  });

  socket.on('disconnect', function onDisconnect() {
    handleSocketDisconnect(socket);
  });
}

// Авторизация, сессии и настройки аккаунта.
function handleAuthRegister(socket, payload, ack) {
  const username = normalizeUsername(payload && payload.username);
  const email = normalizeEmail(payload && payload.email);
  const password = normalizePassword(payload && payload.password);

  const usernameValidationError = validateUsername(username);

  if (usernameValidationError) {
    callAck(ack, { ok: false, error: usernameValidationError });
    return;
  }

  if (!isValidEmail(email)) {
    callAck(ack, { ok: false, error: 'Укажи корректный email адрес.' });
    return;
  }

  const passwordValidationError = validatePassword(password);

  if (passwordValidationError) {
    callAck(ack, { ok: false, error: passwordValidationError });
    return;
  }

  const usernameLower = username.toLowerCase();

  if (registeredAccountIdByUsername.has(usernameLower)) {
    callAck(ack, { ok: false, error: 'Пользователь с таким ником уже зарегистрирован.' });
    return;
  }

  if (registeredAccountIdByEmail.has(email)) {
    callAck(ack, { ok: false, error: 'Аккаунт с таким email уже существует.' });
    return;
  }

  const nowIso = new Date().toISOString();
  const account = {
    id: createId('acc'),
    username,
    usernameLower,
    email,
    passwordHash: bcrypt.hashSync(password, 12),
    avatarUrl: '',
    createdAt: nowIso,
    updatedAt: nowIso,
  };

  registerAccount(account);
  saveAccountsToDisk();

  const authResult = authorizeSocketWithAccount(socket, account);

  if (!authResult.ok) {
    callAck(ack, authResult);
    return;
  }

  const sessionToken = createOrRefreshSessionToken(account.id);
  sessionTokenBySocketId.set(socket.id, sessionToken);

  callAck(ack, {
    ok: true,
    user: authResult.user,
    chats: listChatsForUser(account.id),
    sessionToken,
  });
}

function handleAuthLogin(socket, payload, ack) {
  const username = normalizeUsername(payload && payload.username);
  const password = normalizePassword(payload && payload.password);

  if (!username) {
    callAck(ack, { ok: false, error: 'Укажи ник пользователя.' });
    return;
  }

  if (!password) {
    callAck(ack, { ok: false, error: 'Укажи пароль.' });
    return;
  }

  const account = findAccountByUsername(username);

  if (!account) {
    callAck(ack, { ok: false, error: 'Неверный ник или пароль.' });
    return;
  }

  const isPasswordValid = bcrypt.compareSync(password, account.passwordHash);

  if (!isPasswordValid) {
    callAck(ack, { ok: false, error: 'Неверный ник или пароль.' });
    return;
  }

  const existingSocketId = socketIdByAccountId.get(account.id);

  if (existingSocketId && existingSocketId !== socket.id) {
    callAck(ack, {
      ok: false,
      error: 'Этот аккаунт уже онлайн на другом устройстве.',
    });
    return;
  }

  const authResult = authorizeSocketWithAccount(socket, account);

  if (!authResult.ok) {
    callAck(ack, authResult);
    return;
  }

  const sessionToken = createOrRefreshSessionToken(account.id);
  sessionTokenBySocketId.set(socket.id, sessionToken);

  callAck(ack, {
    ok: true,
    user: authResult.user,
    chats: listChatsForUser(account.id),
    sessionToken,
  });
}

function handleAuthResume(socket, payload, ack) {
  cleanupExpiredSessions();

  const token = normalizeSessionToken(payload && payload.sessionToken);

  if (!token) {
    callAck(ack, { ok: false, error: 'Токен сессии не указан.' });
    return;
  }

  const session = sessionByToken.get(token);

  if (!session || session.expiresAt <= Date.now()) {
    sessionByToken.delete(token);
    callAck(ack, { ok: false, error: 'Сессия истекла. Войди снова.' });
    return;
  }

  const account = registeredAccountsById.get(session.accountId);

  if (!account) {
    sessionByToken.delete(token);
    callAck(ack, { ok: false, error: 'Аккаунт не найден.' });
    return;
  }

  const existingSocketId = socketIdByAccountId.get(account.id);

  if (existingSocketId && existingSocketId !== socket.id) {
    callAck(ack, { ok: false, error: 'Аккаунт уже онлайн на другом устройстве.' });
    return;
  }

  const authResult = authorizeSocketWithAccount(socket, account);

  if (!authResult.ok) {
    callAck(ack, authResult);
    return;
  }

  session.expiresAt = Date.now() + SESSION_TTL_MS;
  sessionByToken.set(token, session);
  sessionTokenBySocketId.set(socket.id, token);

  callAck(ack, {
    ok: true,
    user: authResult.user,
    chats: listChatsForUser(account.id),
    sessionToken: token,
  });
}

function handleAuthLogout(socket, ack) {
  const user = onlineSessionsBySocketId.get(socket.id);
  const sessionToken = sessionTokenBySocketId.get(socket.id);

  if (sessionToken) {
    sessionByToken.delete(sessionToken);
    sessionTokenBySocketId.delete(socket.id);
  }

  if (!user) {
    callAck(ack, { ok: true });
    return;
  }

  onlineSessionsBySocketId.delete(socket.id);

  if (socketIdByAccountId.get(user.id) === socket.id) {
    socketIdByAccountId.delete(user.id);
  }

  callsByChatId.forEach(function handleLeaveByLogout(callSession, chatId) {
    if (callSession.participants.has(user.id)) {
      leaveCallSession(chatId, user.id);
    }
  });

  leaveAllChatRooms(socket);
  emitPublicChatUpdates();

  callAck(ack, { ok: true });
}

function handleAuthUpdateSettings(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const account = registeredAccountsById.get(user.id);

  if (!account) {
    callAck(ack, { ok: false, error: 'Аккаунт не найден.' });
    return;
  }

  const requestedUsername = normalizeUsername(payload && payload.username);
  const requestedEmail = normalizeEmail(payload && payload.email);
  const currentPassword = normalizePassword(payload && payload.currentPassword);
  const newPassword = normalizePassword(payload && payload.newPassword);
  const hasAvatarField = Boolean(payload && Object.prototype.hasOwnProperty.call(payload, 'avatarUrl'));
  const normalizedAvatarValue = normalizeAvatarUrl(payload && payload.avatarUrl);
  const hasProfileUpdate = Boolean(requestedUsername || requestedEmail);
  const hasPasswordUpdate = Boolean(newPassword);
  const hasAvatarUpdate = hasAvatarField;

  if (!hasProfileUpdate && !hasPasswordUpdate && !hasAvatarUpdate) {
    callAck(ack, { ok: false, error: 'Нет данных для обновления.' });
    return;
  }

  if (hasAvatarField && normalizedAvatarValue === null) {
    callAck(ack, { ok: false, error: 'Некорректный адрес аватара.' });
    return;
  }

  const requiresPasswordCheck = hasProfileUpdate || hasPasswordUpdate;

  if (requiresPasswordCheck) {
    if (!currentPassword) {
      callAck(ack, { ok: false, error: 'Укажи текущий пароль.' });
      return;
    }

    const isCurrentPasswordValid = bcrypt.compareSync(currentPassword, account.passwordHash);

    if (!isCurrentPasswordValid) {
      callAck(ack, { ok: false, error: 'Текущий пароль указан неверно.' });
      return;
    }
  }

  const username = requestedUsername || account.username;
  const email = requestedEmail || account.email;
  const avatarUrl = hasAvatarField ? normalizedAvatarValue : account.avatarUrl || '';
  let usernameLower = account.usernameLower;

  if (hasProfileUpdate) {
    const usernameValidationError = validateUsername(username);

    if (usernameValidationError) {
      callAck(ack, { ok: false, error: usernameValidationError });
      return;
    }

    if (!isValidEmail(email)) {
      callAck(ack, { ok: false, error: 'Укажи корректный email адрес.' });
      return;
    }

    usernameLower = username.toLowerCase();
    const existingUsernameAccountId = registeredAccountIdByUsername.get(usernameLower);

    if (existingUsernameAccountId && existingUsernameAccountId !== account.id) {
      callAck(ack, { ok: false, error: 'Пользователь с таким ником уже зарегистрирован.' });
      return;
    }

    const existingEmailAccountId = registeredAccountIdByEmail.get(email);

    if (existingEmailAccountId && existingEmailAccountId !== account.id) {
      callAck(ack, { ok: false, error: 'Аккаунт с таким email уже существует.' });
      return;
    }
  }

  if (newPassword) {
    const passwordValidationError = validatePassword(newPassword);

    if (passwordValidationError) {
      callAck(ack, { ok: false, error: passwordValidationError });
      return;
    }
  }

  const previousUsernameLower = account.usernameLower;
  const previousEmail = account.email;

  account.username = hasProfileUpdate ? username : account.username;
  account.usernameLower = hasProfileUpdate ? usernameLower : account.usernameLower;
  account.email = hasProfileUpdate ? email : account.email;
  account.avatarUrl = avatarUrl || '';

  if (newPassword) {
    account.passwordHash = bcrypt.hashSync(newPassword, 12);
  }

  account.updatedAt = new Date().toISOString();

  if (hasProfileUpdate && previousUsernameLower !== usernameLower) {
    registeredAccountIdByUsername.delete(previousUsernameLower);
    registeredAccountIdByUsername.set(usernameLower, account.id);
  }

  if (hasProfileUpdate && previousEmail !== email) {
    registeredAccountIdByEmail.delete(previousEmail);
    registeredAccountIdByEmail.set(email, account.id);
  }

  const activeSession = onlineSessionsBySocketId.get(socket.id);

  if (activeSession) {
    activeSession.username = account.username;
    activeSession.email = account.email;
    activeSession.avatarUrl = account.avatarUrl || '';
    onlineSessionsBySocketId.set(socket.id, activeSession);
  }

  saveAccountsToDisk();
  emitChatsWhereAccountIsMember(account.id);

  callAck(ack, {
    ok: true,
    user: {
      id: account.id,
      username: account.username,
      email: account.email,
      avatarUrl: account.avatarUrl || '',
      joinedAt: activeSession ? activeSession.joinedAt : new Date().toISOString(),
    },
  });
}

// Поиск пользователей и создание/управление чатами.
function handleUserSearch(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const query = normalizeUsername(payload && payload.query).toLowerCase();
  const requestedLimit = Number(payload && payload.limit);
  const limit = Number.isFinite(requestedLimit) ? Math.max(1, Math.min(20, requestedLimit)) : 10;

  if (query.length < 2) {
    callAck(ack, { ok: true, users: [] });
    return;
  }

  const users = Array.from(registeredAccountsById.values())
    .filter(function filterByQuery(account) {
      if (!account || account.id === user.id) {
        return false;
      }

      return account.usernameLower.includes(query);
    })
    .sort(function sortByUsername(first, second) {
      return first.username.localeCompare(second.username, 'ru', { sensitivity: 'base' });
    })
    .slice(0, limit)
    .map(function mapAccount(account) {
      return {
        id: account.id,
        username: account.username,
        avatarUrl: account.avatarUrl || '',
      };
    });

  callAck(ack, { ok: true, users });
}

function authorizeSocketWithAccount(socket, account) {
  const currentSession = onlineSessionsBySocketId.get(socket.id);

  if (currentSession) {
    socketIdByAccountId.delete(currentSession.id);
  }

  const sessionUser = {
    id: account.id,
    username: account.username,
    email: account.email,
    avatarUrl: account.avatarUrl || '',
    joinedAt: new Date().toISOString(),
  };

  onlineSessionsBySocketId.set(socket.id, sessionUser);
  socketIdByAccountId.set(account.id, socket.id);

  joinAccountToAccessibleRooms(account.id, socket.id);

  return {
    ok: true,
    user: sessionUser,
  };
}

function handleCreatePrivateChat(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const targetUsername = normalizeUsername(payload && payload.targetUsername);

  if (!targetUsername) {
    callAck(ack, { ok: false, error: 'Укажи ник пользователя для личного чата.' });
    return;
  }

  const targetAccount = findAccountByUsername(targetUsername);

  if (!targetAccount) {
    callAck(ack, { ok: false, error: 'Пользователь с таким ником не найден.' });
    return;
  }

  if (targetAccount.id === user.id) {
    callAck(ack, { ok: false, error: 'Нельзя создать личный чат с самим собой.' });
    return;
  }

  const existingChat = findPrivateChat(user.id, targetAccount.id);

  if (existingChat) {
    callAck(ack, {
      ok: true,
      chat: serializeChatForUser(existingChat, user.id),
    });
    return;
  }

  const newChat = createChat({
    type: 'private',
    title: '',
    isPublic: false,
    createdBy: user.id,
    members: [user.id, targetAccount.id],
  });

  joinMembersToRoom(newChat);
  emitChatUpsertToMembers(newChat);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(newChat, user.id),
  });
}

function handleCreateGroupChat(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const title = normalizeTitle(payload && payload.title);
  const rawMemberUsernames = normalizeMemberUsernameList(payload && payload.memberUsernames);

  if (title.length < 3 || title.length > 50) {
    callAck(ack, {
      ok: false,
      error: 'Название группового чата должно быть от 3 до 50 символов.',
    });
    return;
  }

  const membersSet = new Set([user.id]);
  const unknownUsernames = [];

  rawMemberUsernames.forEach(function addByUsername(username) {
    const account = findAccountByUsername(username);

    if (!account) {
      unknownUsernames.push(username);
      return;
    }

    if (account.id !== user.id) {
      membersSet.add(account.id);
    }
  });

  if (unknownUsernames.length > 0) {
    callAck(ack, {
      ok: false,
      error: `Пользователи не найдены: ${unknownUsernames.join(', ')}`,
    });
    return;
  }

  if (membersSet.size < 2) {
    callAck(ack, {
      ok: false,
      error: 'Добавь хотя бы одного существующего пользователя в группу.',
    });
    return;
  }

  const groupChat = createChat({
    type: 'group',
    title,
    isPublic: false,
    createdBy: user.id,
    members: Array.from(membersSet),
    admins: [user.id],
  });

  joinMembersToRoom(groupChat);
  emitChatUpsertToMembers(groupChat);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(groupChat, user.id),
  });
}

function handleCreateChannel(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const title = normalizeTitle(payload && payload.title);

  if (title.length < 3 || title.length > 50) {
    callAck(ack, {
      ok: false,
      error: 'Название канала должно быть от 3 до 50 символов.',
    });
    return;
  }

  const isPublic = Boolean(payload && payload.isPublic !== false);
  const inviteCode = isPublic ? '' : createChannelInviteCode();

  const channel = createChat({
    type: 'channel',
    title,
    isPublic,
    inviteCode,
    createdBy: user.id,
    members: [user.id],
    admins: [user.id],
  });

  joinMembersToRoom(channel);
  emitChatUpsertToMembers(channel);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(channel, user.id),
    inviteCode,
  });
}

function handleChannelSearch(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const query = normalizeTitle(payload && payload.query);
  const queryLower = query.toLowerCase();
  const inviteCode = normalizeInviteCode(query);

  const channels = Array.from(chatsById.values())
    .filter(function filterChannel(chat) {
      if (!chat || chat.type !== 'channel') {
        return false;
      }

      if (chat.members.has(user.id)) {
        return false;
      }

      if (chat.isPublic) {
        if (!queryLower) {
          return false;
        }

        return String(chat.title || '').toLowerCase().includes(queryLower);
      }

      return Boolean(inviteCode && chat.inviteCode && chat.inviteCode === inviteCode);
    })
    .sort(function sortByActivity(first, second) {
      return new Date(second.updatedAt).getTime() - new Date(first.updatedAt).getTime();
    })
    .slice(0, 30)
    .map(function mapChannel(chat) {
      return {
        id: chat.id,
        title: chat.title,
        isPublic: chat.isPublic,
        inviteCode: chat.isPublic ? '' : inviteCode,
        memberCount: chat.members.size,
        createdAt: chat.createdAt,
        updatedAt: chat.updatedAt,
      };
    });

  callAck(ack, {
    ok: true,
    channels,
  });
}

function handleChannelSubscribe(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const channelId = normalizeId(payload && payload.channelId);
  const channel = chatsById.get(channelId);

  if (!channel || channel.type !== 'channel') {
    callAck(ack, { ok: false, error: 'Канал не найден.' });
    return;
  }

  if (!channel.isPublic) {
    const inviteCode = normalizeInviteCode(payload && payload.inviteCode);

    if (!inviteCode || inviteCode !== channel.inviteCode) {
      callAck(ack, { ok: false, error: 'Для подписки на приватный канал нужен корректный код приглашения.' });
      return;
    }
  }

  if (!channel.members.has(user.id)) {
    channel.members.add(user.id);
    channel.updatedAt = new Date().toISOString();
    socket.join(channel.id);
    scheduleChatSave();
  }

  emitChatUpsertToMembers(channel);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(channel, user.id),
  });
}

function handleChatMembers(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат недоступен.' });
    return;
  }

  if (chat.type !== 'group' && chat.type !== 'channel') {
    callAck(ack, { ok: false, error: 'Управление участниками доступно только для групп и каналов.' });
    return;
  }

  const members = Array.from(chat.members).map(function mapMember(memberId) {
    return {
      id: memberId,
      username: getUsernameByAccountId(memberId) || 'Unknown',
      avatarUrl: getAvatarUrlByAccountId(memberId),
      isAdmin: isChatAdmin(chat, memberId),
      isCreator: chat.createdBy === memberId,
    };
  });

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(chat, user.id),
    members,
  });
}

function handleChatMemberRemove(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const memberId = normalizeId(payload && payload.memberId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат недоступен.' });
    return;
  }

  if (chat.type !== 'group' && chat.type !== 'channel') {
    callAck(ack, { ok: false, error: 'Эта операция доступна только в группах и каналах.' });
    return;
  }

  if (!isChatAdmin(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Только админ может исключать участников.' });
    return;
  }

  if (!chat.members.has(memberId)) {
    callAck(ack, { ok: false, error: 'Пользователь уже не состоит в чате.' });
    return;
  }

  if (memberId === chat.createdBy) {
    callAck(ack, { ok: false, error: 'Нельзя исключить создателя чата.' });
    return;
  }

  chat.members.delete(memberId);
  chat.admins.delete(memberId);
  chat.updatedAt = new Date().toISOString();
  scheduleChatSave();

  const memberSocketId = socketIdByAccountId.get(memberId);
  if (memberSocketId) {
    const memberSocket = io.sockets.sockets.get(memberSocketId);

    if (memberSocket) {
      memberSocket.leave(chat.id);
      io.to(memberSocketId).emit('chat:removed', { chatId: chat.id });
    }
  }

  const callSession = callsByChatId.get(chat.id);
  if (callSession && callSession.participants.has(memberId)) {
    leaveCallSession(chat.id, memberId);
  }

  emitChatUpsertToMembers(chat);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(chat, user.id),
  });
}

function handleChatAdminAdd(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const memberId = normalizeId(payload && payload.memberId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат недоступен.' });
    return;
  }

  if (chat.type !== 'group' && chat.type !== 'channel') {
    callAck(ack, { ok: false, error: 'Эта операция доступна только в группах и каналах.' });
    return;
  }

  if (!isChatAdmin(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Только админ может назначать админов.' });
    return;
  }

  if (!chat.members.has(memberId)) {
    callAck(ack, { ok: false, error: 'Пользователь не состоит в чате.' });
    return;
  }

  chat.admins.add(memberId);
  chat.updatedAt = new Date().toISOString();
  scheduleChatSave();

  emitChatUpsertToMembers(chat);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(chat, user.id),
  });
}

function handleChatAdminRemove(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const memberId = normalizeId(payload && payload.memberId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат недоступен.' });
    return;
  }

  if (chat.type !== 'group' && chat.type !== 'channel') {
    callAck(ack, { ok: false, error: 'Эта операция доступна только в группах и каналах.' });
    return;
  }

  if (!isChatAdmin(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Только админ может управлять правами админов.' });
    return;
  }

  if (!chat.admins.has(memberId)) {
    callAck(ack, { ok: false, error: 'Пользователь не является админом.' });
    return;
  }

  if (memberId === chat.createdBy) {
    callAck(ack, { ok: false, error: 'Нельзя снять права у создателя чата.' });
    return;
  }

  if (chat.admins.size <= 1) {
    callAck(ack, { ok: false, error: 'В чате должен остаться хотя бы один админ.' });
    return;
  }

  chat.admins.delete(memberId);
  chat.updatedAt = new Date().toISOString();
  scheduleChatSave();

  emitChatUpsertToMembers(chat);

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(chat, user.id),
  });
}

function handleChatList(socket, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  callAck(ack, {
    ok: true,
    chats: listChatsForUser(user.id),
  });
}

// Сообщения, реакции и набор текста.
function handleMessageHistory(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат не найден или нет доступа.' });
    return;
  }

  const allMessages = messagesByChatId.get(chatId) || [];
  const messages = allMessages.slice(-300).map(function mapMessage(message) {
    return ensureMessageReactions(message);
  });

  callAck(ack, {
    ok: true,
    chat: serializeChatForUser(chat, user.id),
    messages,
  });
}

function handleMessageSend(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат не найден или нет доступа.' });
    return;
  }

  if (chat.type === 'channel' && !isChatAdmin(chat, user.id)) {
    callAck(ack, { ok: false, error: 'В канале писать могут только админы.' });
    return;
  }

  const text = normalizeMessageText(payload && payload.text);
  const attachments = normalizeAttachments(payload && payload.attachments);

  if (!text && attachments.length === 0) {
    callAck(ack, { ok: false, error: 'Сообщение не может быть пустым.' });
    return;
  }

  const senderAccount = registeredAccountsById.get(user.id);
  const senderAvatarUrl = senderAccount ? senderAccount.avatarUrl || '' : '';

  const message = {
    id: createId('msg'),
    chatId,
    senderId: user.id,
    senderName: user.username,
    senderAvatarUrl,
    text,
    attachments,
    reactions: {},
    createdAt: new Date().toISOString(),
  };

  const history = messagesByChatId.get(chatId) || [];
  history.push(message);

  if (history.length > 1500) {
    history.shift();
  }

  messagesByChatId.set(chatId, history);

  chat.updatedAt = message.createdAt;
  chat.lastMessage = {
    senderId: user.id,
    senderName: user.username,
    senderAvatarUrl,
    text: text || buildAttachmentSummary(attachments),
    createdAt: message.createdAt,
  };

  io.to(chatId).emit('message:new', message);
  emitChatUpsertToMembers(chat);
  scheduleChatSave();

  callAck(ack, { ok: true, message });
}

function handleMessageReaction(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const messageId = normalizeId(payload && payload.messageId);
  const emoji = normalizeReactionEmoji(payload && payload.emoji);

  if (!chatId || !messageId) {
    callAck(ack, { ok: false, error: 'Сообщение не найдено.' });
    return;
  }

  if (!emoji) {
    callAck(ack, { ok: false, error: 'Некорректная реакция.' });
    return;
  }

  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат не найден или нет доступа.' });
    return;
  }

  const history = messagesByChatId.get(chatId) || [];
  const message = history.find(function findMessage(item) {
    return item && item.id === messageId;
  });

  if (!message) {
    callAck(ack, { ok: false, error: 'Сообщение не найдено.' });
    return;
  }

  message.reactions = normalizeMessageReactions(message.reactions);

  const reactions = message.reactions;
  const users = Array.isArray(reactions[emoji]) ? reactions[emoji].slice() : [];
  const existingIndex = users.indexOf(user.id);

  if (existingIndex === -1) {
    users.push(user.id);
  } else {
    users.splice(existingIndex, 1);
  }

  if (users.length > 0) {
    reactions[emoji] = users;
  } else {
    delete reactions[emoji];
  }

  message.reactions = reactions;
  messagesByChatId.set(chatId, history);
  scheduleChatSave();

  io.to(chatId).emit('message:reaction', {
    chatId,
    messageId: message.id,
    reactions,
  });

  callAck(ack, { ok: true, reactions });
}

function handleTyping(socket, payload) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    return;
  }

  const isTyping = Boolean(payload && payload.isTyping);

  socket.to(chatId).emit('chat:typing', {
    chatId,
    userId: user.id,
    userName: user.username,
    isTyping,
  });
}

// Звонки и проксирование WebRTC-сигналов между участниками.
function handleCallStart(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const mode = normalizeCallMode(payload && payload.mode);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат не найден или нет доступа.' });
    return;
  }

  let callSession = callsByChatId.get(chatId);

  if (!callSession) {
    callSession = {
      chatId,
      mode,
      participants: new Set(),
      startedAt: new Date().toISOString(),
    };

    callsByChatId.set(chatId, callSession);
  }

  io.to(chatId).emit('call:started', {
    chatId,
    mode: callSession.mode,
    startedBy: {
      id: user.id,
      name: user.username,
    },
  });

  socket.to(chatId).emit('call:incoming', {
    chatId,
    mode: callSession.mode,
    from: {
      id: user.id,
      name: user.username,
    },
  });

  callAck(ack, { ok: true, mode: callSession.mode });
}

function handleCallJoin(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Сначала войди в систему.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const requestedMode = normalizeCallMode(payload && payload.mode);
  const chat = chatsById.get(chatId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат не найден или нет доступа.' });
    return;
  }

  let callSession = callsByChatId.get(chatId);

  if (!callSession) {
    callSession = {
      chatId,
      mode: requestedMode,
      participants: new Set(),
      startedAt: new Date().toISOString(),
    };

    callsByChatId.set(chatId, callSession);
  }

  if (!callSession.participants.has(user.id)) {
    callSession.participants.add(user.id);
  }

  const existingParticipants = Array.from(callSession.participants)
    .filter(function filterParticipant(participantId) {
      return participantId !== user.id;
    })
    .map(serializeUserByAccountId)
    .filter(Boolean);

  socket.to(chatId).emit('call:participantJoined', {
    chatId,
    participant: {
      id: user.id,
      name: user.username,
    },
  });

  callAck(ack, {
    ok: true,
    chatId,
    mode: callSession.mode,
    participants: existingParticipants,
  });
}

function handleCallLeave(socket, payload, ack) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Пользователь не авторизован.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);

  leaveCallSession(chatId, user.id);

  callAck(ack, { ok: true });
}

function handleWebRtcOffer(socket, payload, ack) {
  relayWebRtcEvent(socket, payload, ack, 'webrtc:offer');
}

function handleWebRtcAnswer(socket, payload, ack) {
  relayWebRtcEvent(socket, payload, ack, 'webrtc:answer');
}

function handleWebRtcIceCandidate(socket, payload, ack) {
  relayWebRtcEvent(socket, payload, ack, 'webrtc:ice-candidate');
}

function relayWebRtcEvent(socket, payload, ack, eventName) {
  const user = getAuthorizedUser(socket);

  if (!user) {
    callAck(ack, { ok: false, error: 'Пользователь не авторизован.' });
    return;
  }

  const chatId = normalizeId(payload && payload.chatId);
  const toUserId = normalizeId(payload && payload.toUserId);
  const chat = chatsById.get(chatId);
  const targetSocketId = socketIdByAccountId.get(toUserId);

  if (!chat || !hasChatAccess(chat, user.id)) {
    callAck(ack, { ok: false, error: 'Чат недоступен.' });
    return;
  }

  if (!targetSocketId) {
    callAck(ack, { ok: false, error: 'Получатель офлайн.' });
    return;
  }

  const callSession = callsByChatId.get(chatId);

  if (!callSession || !callSession.participants.has(user.id) || !callSession.participants.has(toUserId)) {
    callAck(ack, { ok: false, error: 'Оба пользователя должны быть в звонке.' });
    return;
  }

  io.to(targetSocketId).emit(eventName, {
    chatId,
    fromUserId: user.id,
    fromUserName: user.username,
    payload: payload && payload.payload ? payload.payload : null,
  });

  callAck(ack, { ok: true });
}

function handleSocketDisconnect(socket) {
  const user = onlineSessionsBySocketId.get(socket.id);
  sessionTokenBySocketId.delete(socket.id);

  if (!user) {
    return;
  }

  onlineSessionsBySocketId.delete(socket.id);

  if (socketIdByAccountId.get(user.id) === socket.id) {
    socketIdByAccountId.delete(user.id);
  }

  callsByChatId.forEach(function handleCallLeaveByDisconnect(callSession, chatId) {
    if (callSession.participants.has(user.id)) {
      leaveCallSession(chatId, user.id);
    }
  });

}

function leaveCallSession(chatId, userId) {
  const callSession = callsByChatId.get(chatId);

  if (!callSession || !callSession.participants.has(userId)) {
    return;
  }

  callSession.participants.delete(userId);

  const leavingUser = serializeUserByAccountId(userId);

  io.to(chatId).emit('call:participantLeft', {
    chatId,
    participant: leavingUser || { id: userId, name: 'Unknown user' },
  });

  if (callSession.participants.size === 0) {
    callsByChatId.delete(chatId);
    io.to(chatId).emit('call:ended', { chatId });
  }
}

// Рассылка обновлений и синхронизация membership-комнат socket.io.
function emitPublicChatUpdates() {
  chatsById.forEach(function emitPublicChat(chat) {
    if (chat.type === 'channel' && chat.isPublic) {
      emitChatUpsertToMembers(chat);
    }
  });
}

function emitChatUpsertToMembers(chat) {
  chat.members.forEach(function emitForMember(memberId) {
    const socketId = socketIdByAccountId.get(memberId);

    if (!socketId) {
      return;
    }

    io.to(socketId).emit('chat:upsert', {
      chat: serializeChatForUser(chat, memberId),
    });
  });
}

function joinMembersToRoom(chat) {
  chat.members.forEach(function joinMember(memberId) {
    const socketId = socketIdByAccountId.get(memberId);

    if (!socketId) {
      return;
    }

    const memberSocket = io.sockets.sockets.get(socketId);

    if (memberSocket) {
      memberSocket.join(chat.id);
    }
  });
}

function joinAccountToAccessibleRooms(accountId, socketId) {
  const targetSocket = io.sockets.sockets.get(socketId);

  if (!targetSocket) {
    return;
  }

  const userChats = listRawChatsForUser(accountId);

  userChats.forEach(function joinChat(chat) {
    targetSocket.join(chat.id);
  });
}

function leaveAllChatRooms(socket) {
  socket.rooms.forEach(function leaveRoom(roomId) {
    if (roomId !== socket.id) {
      socket.leave(roomId);
    }
  });
}

function emitChatsWhereAccountIsMember(accountId) {
  chatsById.forEach(function iterateChat(chat) {
    if (chat.members.has(accountId)) {
      emitChatUpsertToMembers(chat);
    }
  });
}

function findPrivateChat(firstAccountId, secondAccountId) {
  return Array.from(chatsById.values()).find(function findChat(chat) {
    if (chat.type !== 'private') {
      return false;
    }

    if (chat.members.size !== 2) {
      return false;
    }

    return chat.members.has(firstAccountId) && chat.members.has(secondAccountId);
  });
}

function createChat(params) {
  const nowIso = new Date().toISOString();

  const adminSeed = Array.isArray(params.admins) ? params.admins : [params.createdBy];
  const admins = new Set(adminSeed.filter(Boolean));

  const chat = {
    id: createId(params.type),
    type: params.type,
    title: params.title,
    isPublic: Boolean(params.isPublic),
    inviteCode: params.type === 'channel' ? normalizeInviteCode(params.inviteCode) : '',
    createdBy: params.createdBy,
    createdAt: nowIso,
    updatedAt: nowIso,
    members: new Set(params.members || []),
    admins,
    lastMessage: null,
  };

  chatsById.set(chat.id, chat);
  messagesByChatId.set(chat.id, []);
  scheduleChatSave();

  return chat;
}

function ensureDefaultChannels() {
  const nowIso = new Date().toISOString();
  let createdAny = false;

  if (!chatsById.has('channel-general')) {
    const generalChannel = {
      id: 'channel-general',
      type: 'channel',
      title: 'Общий канал',
      isPublic: true,
      inviteCode: '',
      createdBy: 'system',
      createdAt: nowIso,
      updatedAt: nowIso,
      members: new Set(),
      admins: new Set(['system']),
      lastMessage: null,
    };

    chatsById.set(generalChannel.id, generalChannel);
    messagesByChatId.set(generalChannel.id, []);
    createdAny = true;
  } else if (!messagesByChatId.has('channel-general')) {
    messagesByChatId.set('channel-general', []);
  }

  if (!chatsById.has('channel-news')) {
    const newsChannel = {
      id: 'channel-news',
      type: 'channel',
      title: 'Новости проекта',
      isPublic: true,
      inviteCode: '',
      createdBy: 'system',
      createdAt: nowIso,
      updatedAt: nowIso,
      members: new Set(),
      admins: new Set(['system']),
      lastMessage: null,
    };

    chatsById.set(newsChannel.id, newsChannel);
    messagesByChatId.set(newsChannel.id, []);
    createdAny = true;
  } else if (!messagesByChatId.has('channel-news')) {
    messagesByChatId.set('channel-news', []);
  }

  if (createdAny) {
    scheduleChatSave();
  }
}

function listRawChatsForUser(accountId) {
  return Array.from(chatsById.values())
    .filter(function filterChat(chat) {
      return hasChatAccess(chat, accountId);
    })
    .sort(function sortChat(firstChat, secondChat) {
      return new Date(secondChat.updatedAt).getTime() - new Date(firstChat.updatedAt).getTime();
    });
}

function listChatsForUser(accountId) {
  return listRawChatsForUser(accountId).map(function serialize(chat) {
    return serializeChatForUser(chat, accountId);
  });
}

// Сериализация отдаёт на клиент только то, что реально нужно для UI конкретного пользователя.
function serializeChatForUser(chat, accountId) {
  if (!chat) {
    return null;
  }

  ensureChatAdmins(chat);

  const isAdmin = isChatAdmin(chat, accountId);
  const canSendMessages = chat.type === 'channel' ? isAdmin : hasChatAccess(chat, accountId);
  const canManageMembers = (chat.type === 'group' || chat.type === 'channel') && isAdmin;

  let title = chat.title;
  const memberProfiles = Array.from(chat.members)
    .map(function mapMember(memberId) {
      const username = getUsernameByAccountId(memberId);

      if (!username) {
        return null;
      }

      return {
        id: memberId,
        username,
        avatarUrl: getAvatarUrlByAccountId(memberId),
        isAdmin: chat.admins.has(memberId),
        isCreator: chat.createdBy === memberId,
      };
    })
    .filter(Boolean);

  if (chat.type === 'private') {
    const otherAccountId = Array.from(chat.members).find(function findOther(memberId) {
      return memberId !== accountId;
    });

    const otherUsername = otherAccountId ? getUsernameByAccountId(otherAccountId) : null;
    title = otherUsername || 'Личный чат';
  }

  return {
    id: chat.id,
    type: chat.type,
    title,
    isPublic: chat.isPublic,
    inviteCode: chat.type === 'channel' && !chat.isPublic && isAdmin ? chat.inviteCode || '' : '',
    members: Array.from(chat.members),
    memberCount: chat.members.size,
    memberProfiles,
    updatedAt: chat.updatedAt,
    createdAt: chat.createdAt,
    lastMessage: chat.lastMessage,
    adminIds: Array.from(chat.admins),
    isAdmin,
    canSendMessages,
    canManageMembers,
  };
}

function hasChatAccess(chat, accountId) {
  if (!chat) {
    return false;
  }

  return chat.members.has(accountId);
}

function ensureChatAdmins(chat) {
  if (chat.admins instanceof Set) {
    return;
  }

  const seedAdmin = chat.createdBy ? [chat.createdBy] : [];
  chat.admins = new Set(seedAdmin);
}

function isChatAdmin(chat, accountId) {
  if (!chat || !accountId) {
    return false;
  }

  ensureChatAdmins(chat);
  return chat.admins.has(accountId);
}

function getAuthorizedUser(socket) {
  return onlineSessionsBySocketId.get(socket.id) || null;
}

function serializeUserByAccountId(accountId) {
  const username = getUsernameByAccountId(accountId);

  if (!username) {
    return null;
  }

  return {
    id: accountId,
    name: username,
    avatarUrl: getAvatarUrlByAccountId(accountId),
  };
}

function getUsernameByAccountId(accountId) {
  const onlineSocketId = socketIdByAccountId.get(accountId);

  if (onlineSocketId) {
    const onlineSession = onlineSessionsBySocketId.get(onlineSocketId);

    if (onlineSession) {
      return onlineSession.username;
    }
  }

  const account = registeredAccountsById.get(accountId);
  return account ? account.username : null;
}

function getAvatarUrlByAccountId(accountId) {
  const onlineSocketId = socketIdByAccountId.get(accountId);

  if (onlineSocketId) {
    const onlineSession = onlineSessionsBySocketId.get(onlineSocketId);

    if (onlineSession) {
      return onlineSession.avatarUrl || '';
    }
  }

  const account = registeredAccountsById.get(accountId);
  return account ? account.avatarUrl || '' : '';
}

// Нормализаторы делают данные из диска и сокетов предсказуемыми до бизнес-логики.
function normalizeIsoDate(value) {
  if (!value) {
    return new Date().toISOString();
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return new Date().toISOString();
  }

  return date.toISOString();
}

function normalizeLastMessage(rawMessage) {
  if (!rawMessage || typeof rawMessage !== 'object') {
    return null;
  }

  const senderId = normalizeId(rawMessage.senderId);
  const senderName = String(rawMessage.senderName || '').trim();
  const senderAvatarUrl = normalizeAvatarUrl(rawMessage.senderAvatarUrl) || '';
  const text = normalizeMessageText(rawMessage.text);
  const createdAt = normalizeIsoDate(rawMessage.createdAt);

  if (!senderId && !senderName && !text) {
    return null;
  }

  return {
    senderId,
    senderName: senderName || 'Unknown',
    senderAvatarUrl,
    text,
    createdAt,
  };
}

function normalizeChatFromDisk(rawChat) {
  if (!rawChat || typeof rawChat !== 'object') {
    return null;
  }

  const id = normalizeId(rawChat.id);
  const type = rawChat.type === 'private' || rawChat.type === 'group' || rawChat.type === 'channel' ? rawChat.type : '';

  if (!id || !type) {
    return null;
  }

  const membersSeed = Array.isArray(rawChat.members) ? rawChat.members : [];
  const adminsSeed = Array.isArray(rawChat.admins)
    ? rawChat.admins
    : Array.isArray(rawChat.adminIds)
      ? rawChat.adminIds
      : [];

  const members = new Set(membersSeed.map(normalizeId).filter(Boolean));
  const adminsNormalized = adminsSeed.map(normalizeId).filter(Boolean);
  const createdBy = normalizeId(rawChat.createdBy);
  const admins = new Set(adminsNormalized.length > 0 ? adminsNormalized : createdBy ? [createdBy] : []);

  const chat = {
    id,
    type,
    title: normalizeTitle(rawChat.title),
    isPublic: Boolean(rawChat.isPublic),
    inviteCode: type === 'channel' ? normalizeInviteCode(rawChat.inviteCode) : '',
    createdBy,
    createdAt: normalizeIsoDate(rawChat.createdAt),
    updatedAt: normalizeIsoDate(rawChat.updatedAt || rawChat.createdAt),
    members,
    admins,
    lastMessage: normalizeLastMessage(rawChat.lastMessage),
  };

  if (chat.createdBy && !chat.members.has(chat.createdBy)) {
    chat.members.add(chat.createdBy);
  }

  return chat;
}

function normalizeMessageHistory(rawHistory, chatId) {
  if (!Array.isArray(rawHistory)) {
    return [];
  }

  return rawHistory
    .map(function mapMessage(rawMessage) {
      if (!rawMessage || typeof rawMessage !== 'object') {
        return null;
      }

      const id = normalizeId(rawMessage.id);

      if (!id) {
        return null;
      }

      const senderId = normalizeId(rawMessage.senderId);
      const senderName = String(rawMessage.senderName || '').trim();
      const senderAvatarUrl = normalizeAvatarUrl(rawMessage.senderAvatarUrl) || '';
      const text = normalizeMessageText(rawMessage.text);
      const attachments = normalizeAttachments(rawMessage.attachments);
      const reactions = normalizeMessageReactions(rawMessage.reactions);
      const createdAt = normalizeIsoDate(rawMessage.createdAt);

      return {
        id,
        chatId: chatId || normalizeId(rawMessage.chatId),
        senderId,
        senderName,
        senderAvatarUrl,
        text,
        attachments,
        reactions,
        createdAt,
      };
    })
    .filter(Boolean);
}

function normalizeAttachments(inputValue) {
  if (!Array.isArray(inputValue)) {
    return [];
  }

  return inputValue
    .map(function mapAttachment(rawAttachment) {
      if (!rawAttachment || typeof rawAttachment !== 'object') {
        return null;
      }

      const url = String(rawAttachment.url || '').trim();

      if (!url) {
        return null;
      }

      const mimeType = String(rawAttachment.mimeType || 'application/octet-stream').trim();

      return {
        url,
        name: String(rawAttachment.name || 'file').trim(),
        mimeType,
        size: Number(rawAttachment.size || 0),
        kind: detectAttachmentKind(mimeType),
      };
    })
    .filter(Boolean);
}

function buildAttachmentSummary(attachments) {
  if (!attachments.length) {
    return '';
  }

  if (attachments.length === 1) {
    return `Файл: ${attachments[0].name}`;
  }

  return `Файлы: ${attachments.length}`;
}

function normalizeCallMode(mode) {
  return mode === 'video' ? 'video' : 'audio';
}

function normalizeMessageText(text) {
  return String(text || '').trim();
}

function normalizeId(value) {
  return String(value || '').trim();
}

function normalizeReactionEmoji(value) {
  const emoji = String(value || '').trim();

  if (!emoji) {
    return '';
  }

  if (!ALLOWED_REACTIONS.has(emoji)) {
    return '';
  }

  return emoji;
}

function normalizeMessageReactions(value) {
  const normalized = {};

  if (!value || typeof value !== 'object') {
    return normalized;
  }

  Object.keys(value).forEach(function mapReaction(emoji) {
    if (!ALLOWED_REACTIONS.has(emoji)) {
      return;
    }

    const rawUsers = Array.isArray(value[emoji]) ? value[emoji] : [];
    const seen = new Set();
    const users = [];

    rawUsers.forEach(function addUser(rawUserId) {
      const userId = normalizeId(rawUserId);
      if (!userId || seen.has(userId)) {
        return;
      }

      seen.add(userId);
      users.push(userId);
    });

    if (users.length > 0) {
      normalized[emoji] = users;
    }
  });

  return normalized;
}

function ensureMessageReactions(message) {
  if (!message || typeof message !== 'object') {
    return message;
  }

  message.reactions = normalizeMessageReactions(message.reactions);
  return message;
}

function normalizeInviteCode(value) {
  const rawValue = String(value || '').trim().toUpperCase();

  if (!rawValue) {
    return '';
  }

  const matchedToken = rawValue.match(/HEX-[A-F0-9]{8}/);
  return matchedToken ? matchedToken[0] : '';
}

function normalizeTitle(value) {
  return String(value || '').trim();
}

function normalizeUsername(value) {
  return String(value || '').trim();
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizePassword(value) {
  return String(value || '').trim();
}

function normalizeSessionToken(value) {
  const token = String(value || '').trim();

  if (!token) {
    return '';
  }

  if (!/^[a-f0-9]{64}$/i.test(token)) {
    return '';
  }

  return token;
}

function normalizeAvatarUrl(value) {
  const avatarUrl = String(value || '').trim();

  if (!avatarUrl) {
    return '';
  }

  if (avatarUrl.startsWith('/uploads/')) {
    return avatarUrl;
  }

  if (/^https?:\/\//i.test(avatarUrl)) {
    try {
      const parsed = new URL(avatarUrl);

      if (parsed.pathname.startsWith('/uploads/')) {
        return parsed.pathname;
      }
    } catch (error) {
      return null;
    }
  }

  return null;
}

function normalizeResetCode(value) {
  return String(value || '').replace(/\D/g, '').slice(0, PASSWORD_RESET_CODE_LENGTH);
}

function normalizeMemberUsernameList(value) {
  if (Array.isArray(value)) {
    return value
      .map(function mapArrayItem(item) {
        return normalizeUsername(item);
      })
      .filter(Boolean);
  }

  if (typeof value === 'string') {
    return value
      .split(/[\n,;]+/)
      .map(function mapStringItem(item) {
        return normalizeUsername(item);
      })
      .filter(Boolean);
  }

  return [];
}

function validateUsername(username) {
  if (username.length < 3 || username.length > 24) {
    return 'Ник должен быть длиной от 3 до 24 символов.';
  }

  if (!/^[\p{L}\p{N}_.-]+$/u.test(username)) {
    return 'Ник может содержать только буквы, цифры, _, -, .';
  }

  return '';
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function validatePassword(password) {
  if (password.length < PASSWORD_MIN_LENGTH) {
    return `Пароль должен быть не короче ${PASSWORD_MIN_LENGTH} символов.`;
  }

  if (password.length > PASSWORD_MAX_LENGTH) {
    return `Пароль должен быть не длиннее ${PASSWORD_MAX_LENGTH} символов.`;
  }

  return '';
}

function isValidResetCode(code) {
  return /^\d{6}$/.test(code);
}

function createUniqueResetCode() {
  let code = createResetCode();

  while (passwordResetRequestsByCode.has(code)) {
    code = createResetCode();
  }

  return code;
}

function createResetCode() {
  const min = 10 ** (PASSWORD_RESET_CODE_LENGTH - 1);
  const max = 10 ** PASSWORD_RESET_CODE_LENGTH - 1;
  return String(Math.floor(Math.random() * (max - min + 1) + min));
}

function cleanupExpiredPasswordResetRequests() {
  const now = Date.now();

  passwordResetRequestsByCode.forEach(function cleanup(request, code) {
    if (!request || request.expiresAt <= now) {
      passwordResetRequestsByCode.delete(code);
    }
  });
}

function removeResetCodesForAccount(accountId) {
  passwordResetRequestsByCode.forEach(function removeCode(request, code) {
    if (request && request.accountId === accountId) {
      passwordResetRequestsByCode.delete(code);
    }
  });
}

// Для одного аккаунта держим только одну живую серверную сессию.
function createOrRefreshSessionToken(accountId) {
  sessionByToken.forEach(function removePreviousSession(session, token) {
    if (session && session.accountId === accountId) {
      sessionByToken.delete(token);
    }
  });

  const token = crypto.randomBytes(32).toString('hex');
  sessionByToken.set(token, {
    accountId,
    createdAt: Date.now(),
    expiresAt: Date.now() + SESSION_TTL_MS,
  });
  return token;
}

function cleanupExpiredSessions() {
  const now = Date.now();

  sessionByToken.forEach(function cleanupSession(session, token) {
    if (!session || session.expiresAt <= now) {
      sessionByToken.delete(token);
    }
  });
}

function buildPasswordResetLink(req, email, code) {
  const host = req.get('host');
  const protocol = req.protocol || (USE_HTTPS ? 'https' : 'http');
  const baseUrl = `${protocol}://${host}`;
  const query = new URLSearchParams({
    recover: '1',
    email,
    code,
  });

  return `${baseUrl}/?${query.toString()}`;
}

async function sendPasswordResetEmail(params) {
  const mailData = await getMailTransport();
  if (!mailData.transporter) {
    throw new Error(
      'SMTP transport is not configured. Set SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS or SMTP_SERVICE.',
    );
  }

  await mailData.transporter.sendMail({
    from: process.env.MAIL_FROM || 'Hexlet Messenger <no-reply@hexlet-messenger.local>',
    to: params.toEmail,
    subject: 'Восстановление пароля Hexlet Messenger',
    text: [
      `Здравствуйте, ${params.username}!`,
      '',
      'Вы запросили восстановление пароля.',
      `Код восстановления: ${params.code}`,
      `Код действует ${Math.floor(PASSWORD_RESET_TTL_MS / 60000)} минут.`,
      '',
      'Либо откройте ссылку для быстрого восстановления:',
      params.resetLink,
      '',
      'Если это были не вы, просто проигнорируйте письмо.',
    ].join('\n'),
  });
}

function getMailTransport() {
  if (mailTransportPromise) {
    return mailTransportPromise;
  }

  if (process.env.SMTP_HOST) {
    const smtpPort = Number(process.env.SMTP_PORT || 587);
    const smtpSecure = String(process.env.SMTP_SECURE || '').toLowerCase() === 'true';

    mailTransportPromise = Promise.resolve({
      mode: 'smtp',
      transporter: nodemailer.createTransport({
        host: process.env.SMTP_HOST,
        port: smtpPort,
        secure: smtpSecure,
        auth: process.env.SMTP_USER
          ? {
              user: process.env.SMTP_USER,
              pass: process.env.SMTP_PASS || '',
            }
          : undefined,
      }),
    });

    return mailTransportPromise;
  }

  if (process.env.SMTP_SERVICE && process.env.SMTP_USER && process.env.SMTP_PASS) {
    mailTransportPromise = Promise.resolve({
      mode: 'smtp-service',
      transporter: nodemailer.createTransport({
        service: process.env.SMTP_SERVICE,
        auth: {
          user: process.env.SMTP_USER,
          pass: process.env.SMTP_PASS,
        },
      }),
    });

    return mailTransportPromise;
  }

  mailTransportPromise = Promise.resolve({
    mode: 'disabled',
    transporter: null,
  });

  return mailTransportPromise;
}

function registerAccount(account) {
  registeredAccountsById.set(account.id, account);
  registeredAccountIdByUsername.set(account.usernameLower, account.id);
  registeredAccountIdByEmail.set(account.email, account.id);
}

function findAccountByUsername(username) {
  const usernameLower = normalizeUsername(username).toLowerCase();
  const accountId = registeredAccountIdByUsername.get(usernameLower);

  if (!accountId) {
    return null;
  }

  return registeredAccountsById.get(accountId) || null;
}

function findAccountByEmail(email) {
  const normalizedEmail = normalizeEmail(email);
  const accountId = registeredAccountIdByEmail.get(normalizedEmail);

  if (!accountId) {
    return null;
  }

  return registeredAccountsById.get(accountId) || null;
}

// Чаты и сообщения живут в JSON-файле, поэтому читаем и пишем их одним слоем.
function loadChatsFromDisk() {
  if (!fs.existsSync(CHATS_FILE)) {
    return;
  }

  try {
    const raw = fs.readFileSync(CHATS_FILE, 'utf8');
    const parsed = JSON.parse(raw);

    if (!parsed || typeof parsed !== 'object') {
      return;
    }

    const rawChats = Array.isArray(parsed.chats) ? parsed.chats : [];
    rawChats.forEach(function registerChat(rawChat) {
      const chat = normalizeChatFromDisk(rawChat);
      if (!chat) {
        return;
      }

      chatsById.set(chat.id, chat);
    });

    const rawMessagesByChatId =
      parsed.messagesByChatId && typeof parsed.messagesByChatId === 'object'
        ? parsed.messagesByChatId
        : {};

    Object.keys(rawMessagesByChatId).forEach(function registerMessages(chatId) {
      if (!chatsById.has(chatId)) {
        return;
      }

      const history = normalizeMessageHistory(rawMessagesByChatId[chatId], chatId);
      messagesByChatId.set(chatId, history);
    });

    chatsById.forEach(function ensureHistory(chat) {
      if (!messagesByChatId.has(chat.id)) {
        messagesByChatId.set(chat.id, []);
      }
    });
  } catch (error) {
    console.error('Failed to load chats file:', error);
  }
}

function scheduleChatSave() {
  if (chatSaveTimer) {
    return;
  }

  // Дебаунс нужен, чтобы не писать файл на каждый message/reaction/member update по отдельности.
  chatSaveTimer = setTimeout(function saveChatData() {
    chatSaveTimer = null;
    saveChatsToDisk();
  }, CHAT_SAVE_DEBOUNCE_MS);
}

function saveChatsToDisk() {
  const serializedChats = Array.from(chatsById.values()).map(function mapChat(chat) {
    ensureChatAdmins(chat);

    return {
      id: chat.id,
      type: chat.type,
      title: chat.title,
      isPublic: Boolean(chat.isPublic),
      inviteCode: chat.type === 'channel' ? normalizeInviteCode(chat.inviteCode) : '',
      createdBy: chat.createdBy,
      createdAt: chat.createdAt,
      updatedAt: chat.updatedAt,
      members: Array.from(chat.members || []),
      admins: Array.from(chat.admins || []),
      lastMessage: chat.lastMessage || null,
    };
  });

  const serializedMessagesByChatId = {};

  messagesByChatId.forEach(function mapHistory(history, chatId) {
    if (!Array.isArray(history)) {
      return;
    }

    serializedMessagesByChatId[chatId] = history
      .map(function mapMessage(message) {
        if (!message || typeof message !== 'object') {
          return null;
        }

        return {
          id: message.id,
          chatId: message.chatId,
          senderId: message.senderId,
          senderName: message.senderName,
          senderAvatarUrl: message.senderAvatarUrl,
          text: message.text,
          attachments: message.attachments,
          reactions: message.reactions,
          createdAt: message.createdAt,
        };
      })
      .filter(Boolean);
  });

  const payload = {
    chats: serializedChats,
    messagesByChatId: serializedMessagesByChatId,
  };

  fs.writeFileSync(CHATS_FILE, JSON.stringify(payload, null, 2), 'utf8');
}

function loadRegisteredAccountsFromDisk() {
  if (!fs.existsSync(USERS_FILE)) {
    fs.writeFileSync(USERS_FILE, '[]', 'utf8');
    return;
  }

  try {
    const raw = fs.readFileSync(USERS_FILE, 'utf8');
    const parsed = JSON.parse(raw);

    if (!Array.isArray(parsed)) {
      return;
    }

    parsed.forEach(function registerFromFile(item) {
      if (!item || typeof item !== 'object') {
        return;
      }

      const id = normalizeId(item.id);
      const username = normalizeUsername(item.username);
      const email = normalizeEmail(item.email);
      const passwordHash = String(item.passwordHash || '').trim();
      const avatarUrl = normalizeAvatarUrl(item.avatarUrl);

      if (!id || !username || !email || !passwordHash) {
        return;
      }

      const usernameLower = username.toLowerCase();

      if (registeredAccountIdByUsername.has(usernameLower)) {
        return;
      }

      const account = {
        id,
        username,
        usernameLower,
        email,
        passwordHash,
        avatarUrl: avatarUrl || '',
        createdAt: item.createdAt || new Date().toISOString(),
        updatedAt: item.updatedAt || new Date().toISOString(),
      };

      registerAccount(account);
    });
  } catch (error) {
    console.error('Failed to load users file:', error);
  }
}

function saveAccountsToDisk() {
  const serialized = Array.from(registeredAccountsById.values()).map(function mapAccount(account) {
    return {
      id: account.id,
      username: account.username,
      email: account.email,
      passwordHash: account.passwordHash,
      avatarUrl: account.avatarUrl || '',
      createdAt: account.createdAt,
      updatedAt: account.updatedAt,
    };
  });

  fs.writeFileSync(USERS_FILE, JSON.stringify(serialized, null, 2), 'utf8');
}

// Служебные хелперы инфраструктуры.
function createId(prefix) {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

function createChannelInviteCode() {
  return `HEX-${crypto.randomBytes(4).toString('hex').toUpperCase()}`;
}

function detectAttachmentKind(mimeType) {
  if (mimeType.startsWith('image/')) {
    return 'image';
  }

  if (mimeType.startsWith('video/')) {
    return 'video';
  }

  if (mimeType.startsWith('audio/')) {
    return 'audio';
  }

  return 'file';
}

function ensureDirectoryExists(directoryPath) {
  if (fs.existsSync(directoryPath)) {
    return;
  }

  fs.mkdirSync(directoryPath, { recursive: true });
}

function createHttpServer(expressApp) {
  if (!USE_HTTPS) {
    return http.createServer(expressApp);
  }

  if (!fs.existsSync(SSL_KEY_PATH) || !fs.existsSync(SSL_CERT_PATH)) {
    console.warn('SSL files were not found. Falling back to HTTP server.');
    return http.createServer(expressApp);
  }

  const key = fs.readFileSync(SSL_KEY_PATH, 'utf8');
  const cert = fs.readFileSync(SSL_CERT_PATH, 'utf8');

  return https.createServer({ key, cert }, expressApp);
}

function getServerAddresses(host, port, protocol) {
  const localhostAddress = `${protocol}://localhost:${port}`;

  if (host !== '0.0.0.0') {
    return [localhostAddress, `${protocol}://${host}:${port}`];
  }

  const networkInterfaces = os.networkInterfaces();
  const lanAddresses = [];

  Object.values(networkInterfaces).forEach(function iterateInterface(addresses) {
    if (!Array.isArray(addresses)) {
      return;
    }

    addresses.forEach(function iterateAddress(addressInfo) {
      if (!addressInfo || addressInfo.internal || addressInfo.family !== 'IPv4') {
        return;
      }

      lanAddresses.push(`${protocol}://${addressInfo.address}:${port}`);
    });
  });

  return [localhostAddress, ...lanAddresses];
}

function callAck(ack, payload) {
  if (typeof ack === 'function') {
    ack(payload);
  }
}
