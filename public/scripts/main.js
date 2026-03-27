// Единое клиентское состояние: сокет, авторизация, чаты, вложения и звонки.
const state = {
  socket: null,
  currentUser: null,
  chatsById: new Map(),
  discoverableChannels: [],
  messagesByChatId: new Map(),
  knownNamesById: new Map(),
  knownAvatarsById: new Map(),
  currentChatId: null,
  pendingChannelToSubscribe: null,
  searchQuery: '',
  channelSearchTimer: null,
  channelSearchNonce: 0,
  selectedFiles: [],
  typingStopTimer: null,
  typingHideTimer: null,
  privateUserSearchTimer: null,
  privateUserSearchNonce: 0,
  selectedAvatarFile: null,
  selectedAvatarPreviewUrl: '',
  activeCall: null,
  incomingCall: null,
  createMenuOpen: false,
  reactionPickerMessageId: null,
  reactionPickerNode: null,
};

const SESSION_TOKEN_STORAGE_KEY = 'hexlet_session_token_v1';
const REACTION_EMOJIS = ['👍', '❤️', '😂', '🔥', '😍', '😮', '😢', '😡', '🎉'];
const REACTION_EMOJI_SET = new Set(REACTION_EMOJIS);

// Кэшируем основные DOM-узлы, чтобы не искать их заново при каждом рендере.
const elements = {
  appShell: document.getElementById('app-shell'),
  mobileSidebarToggle: document.getElementById('mobile-sidebar-toggle'),
  mobileSidebarClose: document.getElementById('mobile-sidebar-close'),
  sidebarBackdrop: document.getElementById('sidebar-backdrop'),
  currentUserName: document.getElementById('current-user-name'),
  currentUserAvatar: document.getElementById('current-user-avatar'),
  createMenu: document.getElementById('create-menu'),
  createMenuToggle: document.getElementById('create-menu-toggle'),
  createMenuPanel: document.getElementById('create-menu-panel'),
  chatSearch: document.getElementById('chat-search'),
  chatCount: document.getElementById('chat-count'),
  chatList: document.getElementById('chat-list'),
  currentChatTitle: document.getElementById('current-chat-title'),
  currentChatMeta: document.getElementById('current-chat-meta'),
  audioCallButton: document.getElementById('audio-call-button'),
  videoCallButton: document.getElementById('video-call-button'),
  chatManageButton: document.getElementById('chat-manage-button'),
  messageFeed: document.getElementById('message-feed'),
  messagesEmpty: document.getElementById('messages-empty'),
  messagesList: document.getElementById('messages-list'),
  typingIndicator: document.getElementById('typing-indicator'),
  composerPreview: document.getElementById('composer-preview'),
  composerControls: document.querySelector('.composer-controls'),
  messageInput: document.getElementById('message-input'),
  fileInput: document.getElementById('file-input'),
  attachButton: document.getElementById('attach-button'),
  sendButton: document.getElementById('send-button'),
  channelSubscribeInline: document.getElementById('channel-subscribe-inline'),

  loginModal: document.getElementById('login-modal'),
  authTabLogin: document.getElementById('auth-tab-login'),
  authTabRegister: document.getElementById('auth-tab-register'),
  loginForm: document.getElementById('login-form'),
  registerForm: document.getElementById('register-form'),
  loginUsername: document.getElementById('login-username'),
  loginPassword: document.getElementById('login-password'),
  forgotPasswordLink: document.getElementById('forgot-password-link'),
  registerUsername: document.getElementById('register-username'),
  registerEmail: document.getElementById('register-email'),
  registerPassword: document.getElementById('register-password'),
  registerPasswordRepeat: document.getElementById('register-password-repeat'),
  recoverModal: document.getElementById('recover-modal'),
  recoverRequestForm: document.getElementById('recover-request-form'),
  recoverResetForm: document.getElementById('recover-reset-form'),
  recoverEmail: document.getElementById('recover-email'),
  recoverHaveCodeButton: document.getElementById('recover-have-code-button'),
  recoverCloseButton: document.getElementById('recover-close'),
  resetEmail: document.getElementById('reset-email'),
  resetCode: document.getElementById('reset-code'),
  resetPassword: document.getElementById('reset-password'),
  accountSettingsButton: document.getElementById('account-settings-button'),
  logoutButton: document.getElementById('logout-button'),
  accountModal: document.getElementById('account-modal'),
  accountActionProfile: document.getElementById('account-action-profile'),
  accountActionPassword: document.getElementById('account-action-password'),
  accountActionAvatar: document.getElementById('account-action-avatar'),
  accountProfileForm: document.getElementById('account-profile-form'),
  accountPasswordForm: document.getElementById('account-password-form'),
  accountAvatarForm: document.getElementById('account-avatar-form'),
  accountUsername: document.getElementById('account-username'),
  accountEmail: document.getElementById('account-email'),
  accountProfilePassword: document.getElementById('account-profile-password'),
  accountPasswordCurrent: document.getElementById('account-password-current'),
  accountPasswordNew: document.getElementById('account-password-new'),
  accountPasswordRepeat: document.getElementById('account-password-repeat'),
  accountAvatarPreview: document.getElementById('account-avatar-preview'),
  accountAvatarPlaceholder: document.getElementById('account-avatar-placeholder'),
  accountAvatarFile: document.getElementById('account-avatar-file'),
  accountAvatarPick: document.getElementById('account-avatar-pick'),
  accountAvatarRemove: document.getElementById('account-avatar-remove'),
  accountCancel: document.getElementById('account-cancel'),

  createChatModal: document.getElementById('create-chat-modal'),
  createChatType: document.getElementById('create-chat-type'),
  createChatDescription: document.getElementById('create-chat-description'),
  createChatTargetWrap: document.getElementById('create-chat-target-wrap'),
  createChatTargetUsername: document.getElementById('create-chat-target-username'),
  createChatUserResults: document.getElementById('create-chat-user-results'),
  createChatTitleWrap: document.getElementById('create-chat-title-wrap'),
  createChatName: document.getElementById('create-chat-name'),
  createChatMembersWrap: document.getElementById('create-chat-members-wrap'),
  createChatMembersInput: document.getElementById('create-chat-members-input'),
  createChatChannelVisibilityWrap: document.getElementById('create-chat-channel-visibility-wrap'),
  createChatChannelVisibility: document.getElementById('create-chat-channel-visibility'),
  createChatConfirm: document.getElementById('create-chat-confirm'),
  createChatCancel: document.getElementById('create-chat-cancel'),
  newPrivateChat: document.getElementById('new-private-chat'),
  newGroupChat: document.getElementById('new-group-chat'),
  newChannelChat: document.getElementById('new-channel-chat'),
  manageChatModal: document.getElementById('manage-chat-modal'),
  manageChatTitle: document.getElementById('manage-chat-title'),
  manageChatDescription: document.getElementById('manage-chat-description'),
  manageChatMembers: document.getElementById('manage-chat-members'),
  manageChatClose: document.getElementById('manage-chat-close'),

  incomingCall: document.getElementById('incoming-call'),
  incomingCallText: document.getElementById('incoming-call-text'),
  acceptCall: document.getElementById('accept-call'),
  declineCall: document.getElementById('decline-call'),
  callPanel: document.getElementById('call-panel'),
  callChatTitle: document.getElementById('call-chat-title'),
  callStatus: document.getElementById('call-status'),
  leaveCall: document.getElementById('leave-call'),
  localVideo: document.getElementById('local-video'),
  remoteVideos: document.getElementById('remote-videos'),

  mediaModal: document.getElementById('media-modal'),
  mediaModalBackdrop: document.getElementById('media-modal-backdrop'),
  mediaModalClose: document.getElementById('media-modal-close'),
  mediaModalContent: document.getElementById('media-modal-content'),
  mediaModalName: document.getElementById('media-modal-name'),
  mediaModalDownload: document.getElementById('media-modal-download'),

  toast: document.getElementById('toast'),
};

initializeApplication();

// Старт приложения: подключение к сокету, привязка UI и первичная синхронизация интерфейса.
function initializeApplication() {
  connectSocket();
  bindBaseEvents();
  setAuthView('login');
  syncMobileLayout();
  setComposerEnabled(false);
  updateAccountControlsState();
  updateCallButtonsState();
  updateManageChatButtonState();
  refreshCreateModalByType();
  applyRecoveryStateFromQuery();
}

function connectSocket() {
  state.socket = io();

  state.socket.on('connect', handleSocketConnect);
  state.socket.on('disconnect', handleSocketDisconnect);
  state.socket.on('chat:upsert', handleChatUpsertEvent);
  state.socket.on('chat:removed', handleChatRemovedEvent);
  state.socket.on('message:new', handleNewMessageEvent);
  state.socket.on('message:reaction', handleMessageReactionEvent);
  state.socket.on('chat:typing', handleTypingEvent);

  state.socket.on('call:incoming', handleIncomingCallEvent);
  state.socket.on('call:started', handleCallStartedEvent);
  state.socket.on('call:participantJoined', handleCallParticipantJoinedEvent);
  state.socket.on('call:participantLeft', handleCallParticipantLeftEvent);
  state.socket.on('call:ended', handleCallEndedEvent);

  state.socket.on('webrtc:offer', handleWebRtcOfferEvent);
  state.socket.on('webrtc:answer', handleWebRtcAnswerEvent);
  state.socket.on('webrtc:ice-candidate', handleWebRtcIceCandidateEvent);
}

function bindBaseEvents() {
  elements.mobileSidebarToggle.addEventListener('click', openMobileSidebar);
  elements.mobileSidebarClose.addEventListener('click', closeMobileSidebar);
  elements.sidebarBackdrop.addEventListener('click', closeMobileSidebar);
  elements.createMenuToggle.addEventListener('click', handleCreateMenuToggleClick);
  document.addEventListener('click', handleDocumentClick);

  elements.authTabLogin.addEventListener('click', function onAuthLoginTab() {
    setAuthView('login');
  });
  elements.authTabRegister.addEventListener('click', function onAuthRegisterTab() {
    setAuthView('register');
  });
  elements.forgotPasswordLink.addEventListener('click', handleForgotPasswordClick);

  elements.loginForm.addEventListener('submit', handleLoginFormSubmit);
  elements.registerForm.addEventListener('submit', handleRegisterFormSubmit);
  elements.recoverRequestForm.addEventListener('submit', handleRecoverRequestSubmit);
  elements.recoverResetForm.addEventListener('submit', handleRecoverResetSubmit);
  elements.recoverHaveCodeButton.addEventListener('click', handleRecoverHaveCodeClick);
  elements.recoverCloseButton.addEventListener('click', closeRecoverModal);

  elements.chatSearch.addEventListener('input', handleChatSearchInput);

  elements.newPrivateChat.addEventListener('click', handleOpenPrivateModal);
  elements.newGroupChat.addEventListener('click', handleOpenGroupModal);
  elements.newChannelChat.addEventListener('click', handleOpenChannelModal);

  elements.createChatType.addEventListener('change', handleCreateChatTypeChange);
  elements.createChatTargetUsername.addEventListener('input', handlePrivateTargetInput);
  elements.createChatConfirm.addEventListener('click', handleCreateChatConfirm);
  elements.createChatCancel.addEventListener('click', closeCreateChatModal);
  elements.channelSubscribeInline.addEventListener('click', handleInlineChannelSubscribeClick);

  elements.attachButton.addEventListener('click', handleAttachButtonClick);
  elements.fileInput.addEventListener('change', handleFileInputChange);
  elements.sendButton.addEventListener('click', handleSendMessageClick);
  elements.messageInput.addEventListener('keydown', handleMessageInputKeyDown);
  elements.messageInput.addEventListener('input', handleMessageInputTyping);

  elements.audioCallButton.addEventListener('click', handleStartAudioCall);
  elements.videoCallButton.addEventListener('click', handleStartVideoCall);
  elements.chatManageButton.addEventListener('click', handleOpenManageChatModal);

  elements.acceptCall.addEventListener('click', handleAcceptIncomingCall);
  elements.declineCall.addEventListener('click', handleDeclineIncomingCall);
  elements.leaveCall.addEventListener('click', handleLeaveCallClick);
  elements.manageChatClose.addEventListener('click', closeManageChatModal);

  elements.accountSettingsButton.addEventListener('click', handleOpenAccountSettings);
  elements.accountActionProfile.addEventListener('click', function onProfileAccountAction() {
    setAccountView('profile');
  });
  elements.accountActionPassword.addEventListener('click', function onPasswordAccountAction() {
    setAccountView('password');
  });
  elements.accountActionAvatar.addEventListener('click', function onAvatarAccountAction() {
    setAccountView('avatar');
  });
  elements.accountProfileForm.addEventListener('submit', handleAccountProfileSubmit);
  elements.accountPasswordForm.addEventListener('submit', handleAccountPasswordSubmit);
  elements.accountAvatarForm.addEventListener('submit', handleAccountAvatarSubmit);
  elements.accountAvatarPick.addEventListener('click', handleAccountAvatarPickClick);
  elements.accountAvatarFile.addEventListener('change', handleAccountAvatarFileChange);
  elements.accountAvatarRemove.addEventListener('click', handleAccountAvatarRemove);
  elements.accountCancel.addEventListener('click', closeAccountModal);
  elements.logoutButton.addEventListener('click', handleLogoutClick);

  if (elements.mediaModalBackdrop) {
    elements.mediaModalBackdrop.addEventListener('click', closeMediaModal);
  }

  if (elements.mediaModalClose) {
    elements.mediaModalClose.addEventListener('click', closeMediaModal);
  }

  window.addEventListener('beforeunload', handleBeforeUnload);
  window.addEventListener('resize', handleWindowResize);
  window.addEventListener('keydown', handleGlobalKeyDown);
}

async function handleSocketConnect() {
  const resumed = await tryResumeSession();

  if (resumed) {
    return;
  }

  if (!state.currentUser) {
    elements.loginModal.hidden = false;
  }

  showToast('Соединение с сервером установлено');
}

function handleSocketDisconnect() {
  showToast('Связь с сервером потеряна');
}

async function tryResumeSession() {
  const sessionToken = loadSessionToken();

  if (!sessionToken) {
    return false;
  }

  const response = await emitWithAck('auth:resume', {
    sessionToken,
  });

  if (!response || !response.ok) {
    clearSessionToken();
    return false;
  }

  applyAuthSession(response, { silent: true });
  return true;
}

// Переключение между формами входа и регистрации.
function setAuthView(view) {
  const targetView = view === 'register' ? 'register' : 'login';
  elements.authTabLogin.classList.toggle('active', targetView === 'login');
  elements.authTabRegister.classList.toggle('active', targetView === 'register');

  elements.loginForm.hidden = targetView !== 'login';
  elements.registerForm.hidden = targetView !== 'register';
}

async function handleLoginFormSubmit(event) {
  event.preventDefault();

  const username = normalizeText(elements.loginUsername.value);
  const password = String(elements.loginPassword.value || '').trim();

  if (!username || !password) {
    showToast('Введи ник и пароль');
    return;
  }

  const response = await emitWithAck('auth:login', {
    username,
    password,
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось войти в аккаунт');
    return;
  }

  applyAuthSession(response);
}

async function handleRegisterFormSubmit(event) {
  event.preventDefault();

  const username = normalizeText(elements.registerUsername.value);
  const email = normalizeText(elements.registerEmail.value).toLowerCase();
  const password = String(elements.registerPassword.value || '').trim();
  const passwordRepeat = String(elements.registerPasswordRepeat.value || '').trim();

  if (!username || !email || !password || !passwordRepeat) {
    showToast('Заполни все поля регистрации');
    return;
  }

  if (password !== passwordRepeat) {
    showToast('Пароли не совпадают');
    return;
  }

  const response = await emitWithAck('auth:register', {
    username,
    email,
    password,
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось зарегистрировать аккаунт');
    return;
  }

  applyAuthSession(response);
  showToast('Аккаунт создан. Вы вошли в систему.');
}

async function handleRecoverRequestSubmit(event) {
  event.preventDefault();

  const email = normalizeText(elements.recoverEmail.value).toLowerCase();

  if (!email) {
    showToast('Укажи email для восстановления');
    return;
  }

  const response = await fetch('/api/auth/request-password-reset', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      email,
    }),
  });

  const payload = await safeParseJson(response);

  if (!response.ok || !payload.ok) {
    showToast(payload && payload.error ? payload.error : 'Не удалось отправить код восстановления');
    return;
  }

  showToast(payload.message || 'Письмо для восстановления отправлено');

  revealRecoverResetForm();
  elements.resetEmail.value = email;
}

async function handleRecoverResetSubmit(event) {
  event.preventDefault();

  const email = normalizeText(elements.resetEmail.value).toLowerCase();
  const code = String(elements.resetCode.value || '').replace(/\D/g, '').slice(0, 6);
  const newPassword = String(elements.resetPassword.value || '').trim();

  if (!email || !code || !newPassword) {
    showToast('Заполни email, код и новый пароль');
    return;
  }

  const response = await fetch('/api/auth/reset-password', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      email,
      code,
      newPassword,
    }),
  });

  const payload = await safeParseJson(response);

  if (!response.ok || !payload.ok) {
    showToast(payload && payload.error ? payload.error : 'Не удалось сменить пароль');
    return;
  }

  showToast(payload.message || 'Пароль изменен. Теперь можно войти.');

  closeRecoverModal();
  elements.loginPassword.value = '';
  setAuthView('login');
}

// После успешной авторизации синхронизируем локальное состояние и основную часть UI.
function applyAuthSession(response, options) {
  const settings = options || {};

  state.currentUser = response.user;
  state.knownNamesById.set(state.currentUser.id, state.currentUser.username);
  state.knownAvatarsById.set(state.currentUser.id, state.currentUser.avatarUrl || '');

  if (response.sessionToken) {
    saveSessionToken(response.sessionToken);
  }

  replaceChats(response.chats || []);
  state.discoverableChannels = [];
  state.pendingChannelToSubscribe = null;
  hideInlineChannelSubscribeButton();

  elements.currentUserName.textContent = `@${state.currentUser.username}`;
  renderCurrentUserAvatar();
  elements.loginModal.hidden = true;
  closeRecoverModal();
  closeAccountModal();
  closeCreateMenu();
  closeManageChatModal();
  updateAccountControlsState();

  renderChatList();

  if (state.currentChatId) {
    selectChat(state.currentChatId);
  }

  updateManageChatButtonState();

  if (!settings.silent) {
    showToast('Вход выполнен');
  }
}

function handleForgotPasswordClick() {
  openRecoverModal();
}

function openRecoverModal() {
  elements.recoverModal.hidden = false;
}

function closeRecoverModal() {
  elements.recoverModal.hidden = true;
  elements.recoverResetForm.hidden = true;
}

function revealRecoverResetForm() {
  elements.recoverResetForm.hidden = false;
}

function handleRecoverHaveCodeClick() {
  revealRecoverResetForm();
}

function applyRecoveryStateFromQuery() {
  const params = new URLSearchParams(window.location.search);
  const hasRecoverFlag = params.get('recover') === '1';
  const emailFromQuery = normalizeText(params.get('email')).toLowerCase();
  const codeFromQuery = String(params.get('code') || '').replace(/\D/g, '').slice(0, 6);

  if (!hasRecoverFlag && !codeFromQuery) {
    return;
  }

  openRecoverModal();
  revealRecoverResetForm();

  if (emailFromQuery) {
    elements.recoverEmail.value = emailFromQuery;
    elements.resetEmail.value = emailFromQuery;
  }

  if (codeFromQuery) {
    elements.resetCode.value = codeFromQuery;
  }

  params.delete('recover');
  params.delete('email');
  params.delete('code');
  const nextQuery = params.toString();
  const nextUrl = nextQuery ? `${window.location.pathname}?${nextQuery}` : window.location.pathname;
  window.history.replaceState({}, document.title, nextUrl);
}

function updateAccountControlsState() {
  const enabled = Boolean(state.currentUser);
  elements.accountSettingsButton.disabled = !enabled;
  elements.logoutButton.disabled = !enabled;
}

function handleOpenAccountSettings() {
  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  closeMobileSidebar();
  elements.accountUsername.value = state.currentUser.username || '';
  elements.accountEmail.value = state.currentUser.email || '';
  elements.accountProfilePassword.value = '';
  elements.accountPasswordCurrent.value = '';
  elements.accountPasswordNew.value = '';
  elements.accountPasswordRepeat.value = '';
  clearPendingAvatarSelection();
  renderAccountAvatarPreview(state.currentUser.avatarUrl || '');
  setAccountView('profile');
  elements.accountModal.hidden = false;
}

function closeAccountModal() {
  elements.accountModal.hidden = true;
  clearPendingAvatarSelection();
}

// Модалка аккаунта разбита на независимые вкладки: профиль, пароль и аватар.
function setAccountView(view) {
  const targetView = ['profile', 'password', 'avatar'].includes(view) ? view : 'profile';
  elements.accountActionProfile.classList.toggle('active', targetView === 'profile');
  elements.accountActionPassword.classList.toggle('active', targetView === 'password');
  elements.accountActionAvatar.classList.toggle('active', targetView === 'avatar');

  elements.accountProfileForm.hidden = targetView !== 'profile';
  elements.accountPasswordForm.hidden = targetView !== 'password';
  elements.accountAvatarForm.hidden = targetView !== 'avatar';
}

async function handleAccountProfileSubmit(event) {
  event.preventDefault();

  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  const username = normalizeText(elements.accountUsername.value);
  const email = normalizeText(elements.accountEmail.value).toLowerCase();
  const currentPassword = String(elements.accountProfilePassword.value || '').trim();

  if (!username || !email || !currentPassword) {
    showToast('Заполни ник, email и текущий пароль');
    return;
  }

  const response = await emitWithAck('auth:updateSettings', {
    username,
    email,
    currentPassword,
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось обновить настройки аккаунта');
    return;
  }

  applyUpdatedUserSession(response.user);
  showToast('Ник и email обновлены');
}

async function handleAccountPasswordSubmit(event) {
  event.preventDefault();

  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  const currentPassword = String(elements.accountPasswordCurrent.value || '').trim();
  const newPassword = String(elements.accountPasswordNew.value || '').trim();
  const newPasswordRepeat = String(elements.accountPasswordRepeat.value || '').trim();

  if (!currentPassword || !newPassword || !newPasswordRepeat) {
    showToast('Заполни все поля для смены пароля');
    return;
  }

  if (newPassword !== newPasswordRepeat) {
    showToast('Новые пароли не совпадают');
    return;
  }

  const response = await emitWithAck('auth:updateSettings', {
    currentPassword,
    newPassword,
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось обновить пароль');
    return;
  }

  elements.accountPasswordCurrent.value = '';
  elements.accountPasswordNew.value = '';
  elements.accountPasswordRepeat.value = '';
  applyUpdatedUserSession(response.user);
  showToast('Пароль успешно изменён');
}

function handleAccountAvatarPickClick() {
  elements.accountAvatarFile.click();
}

function handleAccountAvatarFileChange(event) {
  const [file] = Array.from(event.target.files || []);

  if (!file) {
    return;
  }

  if (!String(file.type || '').startsWith('image/')) {
    showToast('Для аватара выбери изображение');
    elements.accountAvatarFile.value = '';
    return;
  }

  clearPendingAvatarSelection();
  state.selectedAvatarFile = file;
  state.selectedAvatarPreviewUrl = URL.createObjectURL(file);
  renderAccountAvatarPreview(state.selectedAvatarPreviewUrl);
  elements.accountAvatarFile.value = '';
}

async function handleAccountAvatarSubmit(event) {
  event.preventDefault();

  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  if (!state.selectedAvatarFile) {
    showToast('Сначала выбери изображение для аватара');
    return;
  }

  let uploadedAvatar;

  try {
    uploadedAvatar = await uploadSingleFile(state.selectedAvatarFile);
  } catch (error) {
    showToast('Не удалось загрузить аватар');
    return;
  }

  const response = await emitWithAck('auth:updateSettings', {
    avatarUrl: uploadedAvatar.url,
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось сохранить аватар');
    return;
  }

  clearPendingAvatarSelection();
  applyUpdatedUserSession(response.user);
  renderAccountAvatarPreview(state.currentUser.avatarUrl || '');
  showToast('Аватар обновлён');
}

async function handleAccountAvatarRemove() {
  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  const response = await emitWithAck('auth:updateSettings', {
    avatarUrl: '',
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось удалить аватар');
    return;
  }

  clearPendingAvatarSelection();
  applyUpdatedUserSession(response.user);
  renderAccountAvatarPreview('');
  showToast('Аватар удалён');
}

function applyUpdatedUserSession(updatedUser) {
  if (!updatedUser || !state.currentUser) {
    return;
  }

  state.currentUser = {
    ...state.currentUser,
    ...updatedUser,
  };

  state.knownNamesById.set(state.currentUser.id, state.currentUser.username);
  state.knownAvatarsById.set(state.currentUser.id, state.currentUser.avatarUrl || '');
  elements.currentUserName.textContent = `@${state.currentUser.username}`;
  renderCurrentUserAvatar();
  renderChatList();
  updateCurrentChatHeader();
}

function clearPendingAvatarSelection() {
  if (state.selectedAvatarPreviewUrl) {
    URL.revokeObjectURL(state.selectedAvatarPreviewUrl);
  }

  state.selectedAvatarFile = null;
  state.selectedAvatarPreviewUrl = '';
}

function renderAccountAvatarPreview(avatarUrl) {
  const safeAvatarUrl = normalizeAvatarUrl(avatarUrl);

  if (!safeAvatarUrl) {
    elements.accountAvatarPreview.hidden = true;
    elements.accountAvatarPreview.removeAttribute('src');
    elements.accountAvatarPlaceholder.hidden = false;
    return;
  }

  elements.accountAvatarPreview.src = safeAvatarUrl;
  elements.accountAvatarPreview.hidden = false;
  elements.accountAvatarPlaceholder.hidden = true;
}

function renderCurrentUserAvatar() {
  const safeAvatarUrl = normalizeAvatarUrl(state.currentUser && state.currentUser.avatarUrl);

  if (!safeAvatarUrl) {
    elements.currentUserAvatar.hidden = true;
    elements.currentUserAvatar.removeAttribute('src');
    return;
  }

  elements.currentUserAvatar.src = safeAvatarUrl;
  elements.currentUserAvatar.hidden = false;
}

async function handleLogoutClick() {
  if (!state.currentUser) {
    return;
  }

  const response = await emitWithAck('auth:logout', {});

  if (response && response.ok === false) {
    showToast(response.error || 'Не удалось выйти из аккаунта');
    return;
  }

  await endActiveCallLocally();
  performLocalLogout({ clearSession: true });
  showToast('Вы вышли из аккаунта');
}

function performLocalLogout(options) {
  const settings = options || {};

  state.currentUser = null;
  state.currentChatId = null;
  state.pendingChannelToSubscribe = null;
  state.searchQuery = '';
  state.discoverableChannels = [];
  state.channelSearchNonce = 0;
  state.chatsById.clear();
  state.messagesByChatId.clear();
  state.knownNamesById.clear();
  state.knownAvatarsById.clear();
  clearSelectedFiles();
  clearPendingAvatarSelection();
  if (state.channelSearchTimer) {
    clearTimeout(state.channelSearchTimer);
    state.channelSearchTimer = null;
  }

  if (settings.clearSession !== false) {
    clearSessionToken();
  }

  elements.currentUserName.textContent = 'Не авторизован';
  renderCurrentUserAvatar();
  elements.chatSearch.value = '';
  elements.chatCount.textContent = '0';
  elements.chatList.innerHTML = '';
  elements.currentChatTitle.textContent = 'Выбери чат слева';
  elements.currentChatMeta.textContent = 'Личные, групповые и публичные каналы с единым потоком сообщений.';
  elements.messagesList.innerHTML = '';
  elements.messagesEmpty.hidden = false;
  elements.messagesEmpty.textContent = 'Выбери или создай чат, чтобы начать общение в реальном времени.';
  elements.typingIndicator.hidden = true;
  elements.typingIndicator.textContent = '';
  elements.messageInput.value = '';
  elements.loginPassword.value = '';
  closeAccountModal();
  closeRecoverModal();
  closeCreateChatModal();
  closeCreateMenu();
  closeManageChatModal();
  hideInlineChannelSubscribeButton();
  setComposerEnabled(false);
  updateCallButtonsState();
  updateManageChatButtonState();
  updateAccountControlsState();
  setAuthView('login');
  elements.loginModal.hidden = false;
}

// Поиск каналов запускается с задержкой и защищён nonce, чтобы старые ответы не перетирали новые.
function handleChatSearchInput(event) {
  state.searchQuery = normalizeText(event.target.value).toLowerCase();
  renderChatList();
  scheduleDiscoverableChannelSearch();
}

function scheduleDiscoverableChannelSearch() {
  if (!state.currentUser) {
    return;
  }

  if (state.channelSearchTimer) {
    clearTimeout(state.channelSearchTimer);
    state.channelSearchTimer = null;
  }

  if (!state.searchQuery || state.searchQuery.length < 2) {
    state.discoverableChannels = [];
    renderChatList();
    return;
  }

  const query = state.searchQuery;
  state.channelSearchTimer = setTimeout(function runChannelSearch() {
    performDiscoverableChannelSearch(query);
  }, 220);
}

async function performDiscoverableChannelSearch(query) {
  if (!state.currentUser) {
    return;
  }

  const searchNonce = ++state.channelSearchNonce;
  const response = await emitWithAck('channel:search', { query });

  if (searchNonce !== state.channelSearchNonce) {
    return;
  }

  if (!response || !response.ok) {
    state.discoverableChannels = [];
    renderChatList();
    return;
  }

  state.discoverableChannels = (Array.isArray(response.channels) ? response.channels : []).filter(function filterChannel(
    channel
  ) {
    return channel && channel.id && !state.chatsById.has(channel.id);
  });

  renderChatList();
}

function handleCreateMenuToggleClick(event) {
  event.stopPropagation();

  if (state.createMenuOpen) {
    closeCreateMenu();
    return;
  }

  openCreateMenu();
}

function handleDocumentClick(event) {
  const target = event.target instanceof Element ? event.target : null;

  if (state.createMenuOpen && (!target || !elements.createMenu || !elements.createMenu.contains(target))) {
    closeCreateMenu();
  }

  if (
    state.reactionPickerNode &&
    (!target || (!target.closest('.reaction-picker') && !target.closest('.message-bubble')))
  ) {
    closeReactionPicker();
  }
}

function handleGlobalKeyDown(event) {
  if (event.key !== 'Escape') {
    return;
  }

  if (state.reactionPickerNode) {
    closeReactionPicker();
  }

  if (elements.mediaModal && !elements.mediaModal.hidden) {
    closeMediaModal();
  }
}

function openCreateMenu() {
  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  state.createMenuOpen = true;
  elements.createMenuPanel.hidden = false;
  elements.createMenuToggle.setAttribute('aria-expanded', 'true');
}

function closeCreateMenu() {
  state.createMenuOpen = false;
  elements.createMenuPanel.hidden = true;
  elements.createMenuToggle.setAttribute('aria-expanded', 'false');
}

function handleOpenPrivateModal() {
  closeCreateMenu();
  openCreateChatModal('private');
}

function handleOpenGroupModal() {
  closeCreateMenu();
  openCreateChatModal('group');
}

function handleOpenChannelModal() {
  closeCreateMenu();
  openCreateChatModal('channel');
}

function openCreateChatModal(initialType) {
  if (!state.currentUser) {
    showToast('Сначала войди в аккаунт');
    return;
  }

  closeCreateMenu();
  closeMobileSidebar();
  elements.createChatType.value = initialType;
  elements.createChatTargetUsername.value = '';
  elements.createChatName.value = '';
  elements.createChatMembersInput.value = '';
  elements.createChatChannelVisibility.value = 'public';
  hidePrivateUserSearchResults();

  refreshCreateModalByType();
  elements.createChatModal.hidden = false;
}

function closeCreateChatModal() {
  elements.createChatModal.hidden = true;
  hidePrivateUserSearchResults();
  if (state.privateUserSearchTimer) {
    clearTimeout(state.privateUserSearchTimer);
    state.privateUserSearchTimer = null;
  }
}

async function handleInlineChannelSubscribeClick() {
  if (!state.currentUser || !state.pendingChannelToSubscribe) {
    return;
  }

  const targetChannel = state.pendingChannelToSubscribe;
  const response = await emitWithAck('channel:subscribe', {
    channelId: targetChannel.id,
    inviteCode: targetChannel.inviteCode || '',
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось подписаться на канал');
    return;
  }

  state.pendingChannelToSubscribe = null;
  state.discoverableChannels = state.discoverableChannels.filter(function filterOutJoined(channel) {
    return channel.id !== response.chat.id;
  });
  upsertChat(response.chat);
  selectChat(response.chat.id);
  showToast('Подписка оформлена');
}

// Создание чата собирает общий UI, а дальше отправляет нужный сокет-ивент по типу чата.
function handleCreateChatTypeChange() {
  refreshCreateModalByType();
}

function refreshCreateModalByType() {
  const type = elements.createChatType.value;

  elements.createChatTargetWrap.hidden = type !== 'private';
  elements.createChatTitleWrap.hidden = type === 'private';
  elements.createChatMembersWrap.hidden = type !== 'group';
  elements.createChatChannelVisibilityWrap.hidden = type !== 'channel';

  if (type === 'private') {
    elements.createChatDescription.textContent = 'Для личного чата укажи ник пользователя.';
    maybeSearchUsersForPrivateChat();
    return;
  }

  if (type === 'group') {
    elements.createChatDescription.textContent = 'Укажи название и ники участников через запятую.';
    hidePrivateUserSearchResults();
    return;
  }

  elements.createChatDescription.textContent =
    'Создай канал и выбери режим: публичный по названию или приватный по приглашению.';
  hidePrivateUserSearchResults();
}

function handlePrivateTargetInput() {
  maybeSearchUsersForPrivateChat();
}

function maybeSearchUsersForPrivateChat() {
  if (elements.createChatType.value !== 'private') {
    hidePrivateUserSearchResults();
    return;
  }

  const query = normalizeText(elements.createChatTargetUsername.value);

  if (query.length < 2) {
    hidePrivateUserSearchResults();
    return;
  }

  if (state.privateUserSearchTimer) {
    clearTimeout(state.privateUserSearchTimer);
  }

  state.privateUserSearchTimer = setTimeout(function runDelayedUserSearch() {
    performPrivateUserSearch(query);
  }, 180);
}

async function performPrivateUserSearch(query) {
  const searchNonce = ++state.privateUserSearchNonce;
  const response = await emitWithAck('user:search', { query, limit: 8 });

  if (searchNonce !== state.privateUserSearchNonce) {
    return;
  }

  if (!response || !response.ok) {
    hidePrivateUserSearchResults();
    return;
  }

  renderPrivateUserSearchResults(Array.isArray(response.users) ? response.users : []);
}

function renderPrivateUserSearchResults(users) {
  if (users.length === 0) {
    elements.createChatUserResults.hidden = false;
    elements.createChatUserResults.innerHTML = '<p class="user-search-empty">Пользователи не найдены</p>';
    return;
  }

  const fragment = document.createDocumentFragment();

  users.forEach(function appendSearchUser(user) {
    const button = document.createElement('button');
    button.className = 'user-search-item';
    button.type = 'button';

    const avatar = document.createElement('div');
    avatar.className = 'user-search-avatar';
    const safeAvatarUrl = normalizeAvatarUrl(user && user.avatarUrl);

    if (safeAvatarUrl) {
      const image = document.createElement('img');
      image.src = safeAvatarUrl;
      image.alt = user.username || 'avatar';
      avatar.append(image);
    } else {
      avatar.textContent = '👤';
    }

    const username = document.createElement('span');
    username.className = 'user-search-name';
    username.textContent = user.username || '';

    button.append(avatar, username);
    button.addEventListener('click', function onPickUser() {
      elements.createChatTargetUsername.value = user.username || '';
      hidePrivateUserSearchResults();
    });

    fragment.append(button);
  });

  elements.createChatUserResults.innerHTML = '';
  elements.createChatUserResults.append(fragment);
  elements.createChatUserResults.hidden = false;
}

function hidePrivateUserSearchResults() {
  elements.createChatUserResults.hidden = true;
  elements.createChatUserResults.innerHTML = '';
}

async function handleCreateChatConfirm() {
  const type = elements.createChatType.value;

  if (type === 'private') {
    const targetUsername = normalizeText(elements.createChatTargetUsername.value);

    if (!targetUsername) {
      showToast('Укажи ник пользователя для личного чата');
      return;
    }

    const response = await emitWithAck('chat:createPrivate', { targetUsername });

    if (!response || !response.ok) {
      showToast(response && response.error ? response.error : 'Не удалось создать личный чат');
      return;
    }

    upsertChat(response.chat);
    closeCreateChatModal();
    selectChat(response.chat.id);
    return;
  }

  if (type === 'group') {
    const title = normalizeText(elements.createChatName.value);
    const memberUsernames = parseUsernamesInput(elements.createChatMembersInput.value);

    if (title.length < 3) {
      showToast('Название группы должно быть от 3 символов');
      return;
    }

    if (memberUsernames.length === 0) {
      showToast('Укажи хотя бы один ник участника группы');
      return;
    }

    const response = await emitWithAck('chat:createGroup', {
      title,
      memberUsernames,
    });

    if (!response || !response.ok) {
      showToast(response && response.error ? response.error : 'Не удалось создать групповой чат');
      return;
    }

    upsertChat(response.chat);
    closeCreateChatModal();
    selectChat(response.chat.id);
    return;
  }

  const title = normalizeText(elements.createChatName.value);

  if (title.length < 3) {
    showToast('Название канала должно быть от 3 символов');
    return;
  }

  const channelVisibility = elements.createChatChannelVisibility.value === 'private' ? 'private' : 'public';
  const response = await emitWithAck('chat:createChannel', {
    title,
    isPublic: channelVisibility === 'public',
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось создать канал');
    return;
  }

  upsertChat(response.chat);
  closeCreateChatModal();
  selectChat(response.chat.id);

  if (channelVisibility === 'private' && response.inviteCode) {
    showToast(`Приватный канал создан. Код приглашения: ${response.inviteCode}`);
  }
}

// Вложения сначала живут локально в preview, а перед отправкой загружаются через HTTP.
function handleAttachButtonClick() {
  if (!state.currentChatId) {
    showToast('Сначала выбери чат');
    return;
  }

  elements.fileInput.click();
}

function handleFileInputChange(event) {
  const files = Array.from(event.target.files || []);

  if (files.length === 0) {
    return;
  }

  files.forEach(function appendSelectedFile(file) {
    const id = createLocalId();
    const kind = detectAttachmentKind(file.type);
    const previewUrl = URL.createObjectURL(file);

    state.selectedFiles.push({
      id,
      file,
      kind,
      previewUrl,
    });
  });

  elements.fileInput.value = '';
  renderSelectedFilesPreview();
}

function renderSelectedFilesPreview() {
  if (state.selectedFiles.length === 0) {
    elements.composerPreview.innerHTML = '';
    return;
  }

  const fragment = document.createDocumentFragment();

  state.selectedFiles.forEach(function appendPreview(selectedItem) {
    const chip = document.createElement('div');
    chip.className = 'preview-chip';

    const thumbNode = createPreviewThumb(selectedItem);
    const nameNode = document.createElement('p');
    nameNode.className = 'preview-name';
    nameNode.textContent = selectedItem.file.name;

    const removeButton = document.createElement('button');
    removeButton.className = 'preview-remove';
    removeButton.type = 'button';
    removeButton.textContent = '✕';
    removeButton.addEventListener('click', function removePreviewItem() {
      removeSelectedFile(selectedItem.id);
    });

    chip.append(thumbNode, nameNode, removeButton);
    fragment.append(chip);
  });

  elements.composerPreview.innerHTML = '';
  elements.composerPreview.append(fragment);
}

function createPreviewThumb(selectedItem) {
  if (selectedItem.kind === 'image') {
    const image = document.createElement('img');
    image.className = 'preview-thumb';
    image.src = selectedItem.previewUrl;
    image.alt = 'preview';
    return image;
  }

  const placeholder = document.createElement('div');
  placeholder.className = 'preview-thumb';

  if (selectedItem.kind === 'video') {
    placeholder.textContent = '🎬';
  } else if (selectedItem.kind === 'audio') {
    placeholder.textContent = '🎵';
  } else {
    placeholder.textContent = '📄';
  }

  placeholder.style.display = 'grid';
  placeholder.style.placeItems = 'center';

  return placeholder;
}

function removeSelectedFile(fileId) {
  const index = state.selectedFiles.findIndex(function findById(item) {
    return item.id === fileId;
  });

  if (index < 0) {
    return;
  }

  const [removed] = state.selectedFiles.splice(index, 1);
  URL.revokeObjectURL(removed.previewUrl);
  renderSelectedFilesPreview();
}

async function handleSendMessageClick() {
  if (!state.currentChatId || !state.currentUser) {
    return;
  }

  const text = normalizeText(elements.messageInput.value);

  if (!text && state.selectedFiles.length === 0) {
    return;
  }

  elements.sendButton.disabled = true;

  try {
    const uploadedAttachments = await uploadSelectedFiles();
    const response = await emitWithAck('message:send', {
      chatId: state.currentChatId,
      text,
      attachments: uploadedAttachments,
    });

    if (!response || !response.ok) {
      showToast(response && response.error ? response.error : 'Не удалось отправить сообщение');
      return;
    }

    elements.messageInput.value = '';
    clearSelectedFiles();
    sendTypingState(false);
  } catch (error) {
    console.error(error);
    showToast('Ошибка при отправке сообщения');
  } finally {
    elements.sendButton.disabled = false;
  }
}

async function uploadSelectedFiles() {
  if (state.selectedFiles.length === 0) {
    return [];
  }

  const uploadPromises = state.selectedFiles.map(function mapToUploadPromise(selectedItem) {
    return uploadSingleFile(selectedItem.file);
  });

  return Promise.all(uploadPromises);
}

async function uploadSingleFile(file) {
  const formData = new FormData();
  formData.append('file', file);

  const response = await fetch('/api/upload', {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    throw new Error('Upload failed');
  }

  const payload = await response.json();

  if (!payload.ok || !payload.file) {
    throw new Error('Upload payload is invalid');
  }

  return payload.file;
}

function handleMessageInputKeyDown(event) {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    handleSendMessageClick();
  }
}

function handleMessageInputTyping() {
  if (!state.currentChatId) {
    return;
  }

  sendTypingState(true);

  if (state.typingStopTimer) {
    clearTimeout(state.typingStopTimer);
  }

  state.typingStopTimer = setTimeout(function stopTyping() {
    sendTypingState(false);
  }, 900);
}

function sendTypingState(isTyping) {
  if (!state.currentChatId || !state.currentUser) {
    return;
  }

  state.socket.emit('chat:typing', {
    chatId: state.currentChatId,
    isTyping,
  });
}

// Сервер присылает новые чаты/сообщения как дельты, а клиент вшивает их в локальный state.
function handleChatUpsertEvent(payload) {
  if (!payload || !payload.chat) {
    return;
  }

  if (state.pendingChannelToSubscribe && state.pendingChannelToSubscribe.id === payload.chat.id) {
    state.pendingChannelToSubscribe = null;
    hideInlineChannelSubscribeButton();
  }

  upsertChat(payload.chat);

  if (state.currentChatId === payload.chat.id) {
    updateCurrentChatHeader();
  }
}

function handleChatRemovedEvent(payload) {
  if (!payload || !payload.chatId) {
    return;
  }

  state.chatsById.delete(payload.chatId);
  state.messagesByChatId.delete(payload.chatId);
  state.discoverableChannels = state.discoverableChannels.filter(function filterRemoved(channel) {
    return channel.id !== payload.chatId;
  });

  if (state.pendingChannelToSubscribe && state.pendingChannelToSubscribe.id === payload.chatId) {
    state.pendingChannelToSubscribe = null;
    hideInlineChannelSubscribeButton();
  }

  if (state.currentChatId === payload.chatId) {
    state.currentChatId = null;
    closeManageChatModal();
    setComposerEnabled(false);
    updateCurrentChatHeader();
    updateCallButtonsState();
    updateManageChatButtonState();
  }

  renderChatList();
}

function handleNewMessageEvent(message) {
  if (!message || !message.chatId) {
    return;
  }

  message.reactions = normalizeMessageReactions(message.reactions);

  const history = state.messagesByChatId.get(message.chatId) || [];
  history.push(message);
  state.messagesByChatId.set(message.chatId, history);

  if (message.senderId && message.senderName) {
    state.knownNamesById.set(message.senderId, message.senderName);
    state.knownAvatarsById.set(message.senderId, normalizeAvatarUrl(message.senderAvatarUrl || ''));
  }

  const relatedChat = state.chatsById.get(message.chatId);

  if (relatedChat) {
    relatedChat.lastMessage = {
      senderId: message.senderId,
      senderName: message.senderName,
      text: message.text || buildAttachmentSummary(message.attachments || []),
      createdAt: message.createdAt,
    };

    relatedChat.updatedAt = message.createdAt;
    state.chatsById.set(relatedChat.id, relatedChat);
  }

  if (message.chatId === state.currentChatId) {
    renderMessagesForCurrentChat();
    scrollMessagesToBottom();
  }

  renderChatList();
}

function handleMessageReactionEvent(payload) {
  if (!payload || !payload.chatId || !payload.messageId) {
    return;
  }

  applyMessageReactionsUpdate(payload.chatId, payload.messageId, payload.reactions);
}

function applyMessageReactionsUpdate(chatId, messageId, reactions) {
  const history = state.messagesByChatId.get(chatId) || [];
  const message = history.find(function findMessage(item) {
    return item && item.id === messageId;
  });

  if (!message) {
    return;
  }

  message.reactions = normalizeMessageReactions(reactions);
  state.messagesByChatId.set(chatId, history);

  if (chatId === state.currentChatId) {
    const wasAtBottom = isMessageFeedAtBottom();
    const scrollTop = elements.messageFeed.scrollTop;

    renderMessagesForCurrentChat();

    if (wasAtBottom) {
      scrollMessagesToBottom();
    } else {
      elements.messageFeed.scrollTop = scrollTop;
    }
  }
}

function handleTypingEvent(payload) {
  if (!payload || payload.chatId !== state.currentChatId || !payload.isTyping) {
    return;
  }

  elements.typingIndicator.hidden = false;
  elements.typingIndicator.textContent = `${payload.userName} печатает...`;

  if (state.typingHideTimer) {
    clearTimeout(state.typingHideTimer);
  }

  state.typingHideTimer = setTimeout(function hideTyping() {
    elements.typingIndicator.hidden = true;
    elements.typingIndicator.textContent = '';
  }, 1500);
}

function replaceChats(chats) {
  state.chatsById.clear();

  chats.forEach(function addChat(chat) {
    if (chat && chat.id) {
      state.chatsById.set(chat.id, chat);
      rememberChatMembers(chat);
    }
  });

  state.discoverableChannels = state.discoverableChannels.filter(function filterJoined(channel) {
    return channel && channel.id && !state.chatsById.has(channel.id);
  });

  if (state.pendingChannelToSubscribe && state.chatsById.has(state.pendingChannelToSubscribe.id)) {
    state.pendingChannelToSubscribe = null;
    hideInlineChannelSubscribeButton();
  }

  const allChats = getSortedChats();

  if (allChats.length === 0) {
    state.currentChatId = null;
    return;
  }

  if (!state.currentChatId || !state.chatsById.has(state.currentChatId)) {
    state.currentChatId = allChats[0].id;
  }
}

function upsertChat(chat) {
  if (!chat || !chat.id) {
    return;
  }

  state.chatsById.set(chat.id, chat);
  state.discoverableChannels = state.discoverableChannels.filter(function filterUpserted(channel) {
    return channel.id !== chat.id;
  });
  rememberChatMembers(chat);
  renderChatList();

  if (state.currentChatId === chat.id) {
    setComposerEnabled(Boolean(chat.canSendMessages !== false));
    updateCurrentChatHeader();
    updateManageChatButtonState();
  }
}

function rememberChatMembers(chat) {
  if (!chat || !Array.isArray(chat.memberProfiles)) {
    return;
  }

  chat.memberProfiles.forEach(function rememberMember(profile) {
    if (profile && profile.id && profile.username) {
      state.knownNamesById.set(profile.id, profile.username);
      state.knownAvatarsById.set(profile.id, normalizeAvatarUrl(profile.avatarUrl || ''));
    }
  });
}

function getSortedChats() {
  return Array.from(state.chatsById.values()).sort(function sortChat(a, b) {
    const timeA = a.updatedAt ? new Date(a.updatedAt).getTime() : 0;
    const timeB = b.updatedAt ? new Date(b.updatedAt).getTime() : 0;
    return timeB - timeA;
  });
}

function getFilteredChats() {
  const allChats = getSortedChats();

  if (!state.searchQuery) {
    return allChats;
  }

  return allChats.filter(function filterChat(chat) {
    const chatTitle = normalizeText(chat.title).toLowerCase();
    const lastText = chat.lastMessage ? normalizeText(chat.lastMessage.text).toLowerCase() : '';
    return chatTitle.includes(state.searchQuery) || lastText.includes(state.searchQuery);
  });
}

function getFilteredDiscoverableChannels() {
  if (!state.searchQuery || state.searchQuery.length < 2) {
    return [];
  }

  return state.discoverableChannels.filter(function filterChannel(channel) {
    if (!channel || !channel.id || state.chatsById.has(channel.id)) {
      return false;
    }

    const title = normalizeText(channel.title).toLowerCase();
    const inviteCode = normalizeText(channel.inviteCode || '').toLowerCase();
    return title.includes(state.searchQuery) || inviteCode.includes(state.searchQuery);
  });
}

// Левый список объединяет доступные чаты и найденные каналы, на которые пользователь ещё не подписан.
function renderChatList() {
  const chats = getFilteredChats();
  const discoverableChannels = getFilteredDiscoverableChannels();
  elements.chatCount.textContent = String(state.chatsById.size);

  if (chats.length === 0 && discoverableChannels.length === 0) {
    elements.chatList.innerHTML = '<li class="member-option">Чаты не найдены</li>';
    return;
  }

  const fragment = document.createDocumentFragment();

  chats.forEach(function appendChat(chat) {
    const item = document.createElement('li');
    item.className = 'chat-item';

    if (chat.id === state.currentChatId) {
      item.classList.add('active');
    }

    const top = document.createElement('div');
    top.className = 'chat-item-top';

    const title = document.createElement('h3');
    title.className = 'chat-title';
    title.textContent = chat.title;

    const typeTag = document.createElement('span');
    typeTag.className = `chat-type ${chat.type}`;
    typeTag.textContent = getTypeLabel(chat.type);

    top.append(title, typeTag);

    const preview = document.createElement('p');
    preview.className = 'chat-preview';
    preview.textContent = getChatPreviewText(chat);

    item.append(top, preview);
    item.addEventListener('click', function onChatClick() {
      selectChat(chat.id);
    });

    fragment.append(item);
  });

  if (discoverableChannels.length > 0) {
    const separator = document.createElement('li');
    separator.className = 'chat-list-separator';
    separator.textContent = 'Найденные каналы';
    fragment.append(separator);
  }

  discoverableChannels.forEach(function appendDiscoverable(channel) {
    const item = document.createElement('li');
    item.className = 'chat-item discoverable';

    if (state.pendingChannelToSubscribe && state.pendingChannelToSubscribe.id === channel.id) {
      item.classList.add('active');
    }

    const top = document.createElement('div');
    top.className = 'chat-item-top';

    const title = document.createElement('h3');
    title.className = 'chat-title';
    title.textContent = channel.title || 'Канал';

    const typeTag = document.createElement('span');
    typeTag.className = 'chat-type channel';
    typeTag.textContent = channel.isPublic ? 'Публичный' : 'Приватный';

    top.append(title, typeTag);

    const preview = document.createElement('p');
    preview.className = 'chat-preview';
    preview.textContent = channel.isPublic
      ? `Публичный канал • Подписчиков: ${channel.memberCount || 0}`
      : 'Приватный канал • доступ по приглашению';

    item.append(top, preview);
    item.addEventListener('click', function onDiscoverableClick() {
      openChannelDiscoverPreview(channel);
    });

    fragment.append(item);
  });

  elements.chatList.innerHTML = '';
  elements.chatList.append(fragment);
}

function openChannelDiscoverPreview(channel) {
  if (!channel || !channel.id) {
    return;
  }

  closeManageChatModal();
  clearSelectedFiles();
  elements.messageInput.value = '';
  state.currentChatId = null;
  state.pendingChannelToSubscribe = channel;
  updateCurrentChatHeader();
  renderChatList();
  renderMessagesForCurrentChat();
  setComposerEnabled(false);
  updateCallButtonsState();
  updateManageChatButtonState();
  closeMobileSidebar();
}

async function selectChat(chatId) {
  const chat = state.chatsById.get(chatId);

  if (!chat) {
    return;
  }

  closeManageChatModal();
  state.pendingChannelToSubscribe = null;
  hideInlineChannelSubscribeButton();
  state.currentChatId = chatId;

  updateCurrentChatHeader();
  renderChatList();
  setComposerEnabled(Boolean(chat.canSendMessages !== false));
  updateCallButtonsState();
  updateManageChatButtonState();

  const response = await emitWithAck('message:history', { chatId });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось загрузить историю');
    return;
  }

  if (response.chat) {
    upsertChat(response.chat);
    setComposerEnabled(Boolean(response.chat.canSendMessages !== false));
    updateManageChatButtonState();
  }

  const history = Array.isArray(response.messages) ? response.messages : [];

  history.forEach(function rememberMessageAuthor(message) {
    if (message) {
      message.reactions = normalizeMessageReactions(message.reactions);
    }

    if (message && message.senderId && message.senderName) {
      state.knownNamesById.set(message.senderId, message.senderName);
      state.knownAvatarsById.set(message.senderId, normalizeAvatarUrl(message.senderAvatarUrl || ''));
    }
  });

  state.messagesByChatId.set(chatId, history);

  renderMessagesForCurrentChat();
  scrollMessagesToBottom();
  closeMobileSidebar();
}

function updateCurrentChatHeader() {
  if (state.pendingChannelToSubscribe) {
    const previewChannel = state.pendingChannelToSubscribe;
    const metaParts = [];
    metaParts.push(`Тип: ${previewChannel.isPublic ? 'Публичный канал' : 'Приватный канал'}`);
    metaParts.push(`Подписчиков: ${previewChannel.memberCount || 0}`);

    if (previewChannel.isPublic) {
      metaParts.push('Найден в поиске');
    } else {
      metaParts.push('Доступ только по приглашению');
    }

    elements.currentChatTitle.textContent = previewChannel.title || 'Канал';
    elements.currentChatMeta.textContent = metaParts.join(' • ');
    return;
  }

  if (!state.currentChatId) {
    elements.currentChatTitle.textContent = 'Выбери чат слева';
    elements.currentChatMeta.textContent = 'Личные, групповые и публичные каналы с единым потоком сообщений.';
    return;
  }

  const chat = state.chatsById.get(state.currentChatId);

  if (!chat) {
    return;
  }

  const metaParts = [];
  metaParts.push(`Тип: ${getTypeLabel(chat.type)}`);
  metaParts.push(`Участников: ${chat.memberCount || 0}`);

  if (chat.type === 'channel' && !chat.isPublic) {
    metaParts.push('Приватный канал');
  }

  if (chat.type === 'channel' && chat.canSendMessages === false) {
    metaParts.push('Режим: только админы пишут');
  }

  if (chat.type === 'channel' && chat.inviteCode && chat.isAdmin) {
    metaParts.push(`Код приглашения: ${chat.inviteCode}`);
  }

  if (chat.lastMessage && chat.lastMessage.createdAt) {
    metaParts.push(`Активность: ${formatTime(chat.lastMessage.createdAt)}`);
  }

  elements.currentChatTitle.textContent = chat.title;
  elements.currentChatMeta.textContent = metaParts.join(' • ');
}

// Основной рендер сообщений строится из локальной истории, без прямых мутаций HTML-шаблонов.
function renderMessagesForCurrentChat() {
  closeReactionPicker();

  if (state.pendingChannelToSubscribe) {
    elements.messagesList.innerHTML = '';
    elements.messagesEmpty.hidden = false;
    elements.messagesEmpty.textContent = 'Открой канал кнопкой ниже, чтобы подписаться и начать получать сообщения.';
    return;
  }

  if (!state.currentChatId) {
    elements.messagesList.innerHTML = '';
    elements.messagesEmpty.hidden = false;
    return;
  }

  const messages = state.messagesByChatId.get(state.currentChatId) || [];

  if (messages.length === 0) {
    elements.messagesList.innerHTML = '';
    elements.messagesEmpty.hidden = false;
    elements.messagesEmpty.textContent = 'В этом чате пока нет сообщений. Начни разговор первым.';
    return;
  }

  elements.messagesEmpty.hidden = true;

  const fragment = document.createDocumentFragment();

  messages.forEach(function appendMessage(message) {
    const item = document.createElement('li');
    item.className = 'message-item';

    if (state.currentUser && message.senderId === state.currentUser.id) {
      item.classList.add('mine');
    }

    const stack = document.createElement('div');
    stack.className = 'message-stack';

    const bubble = document.createElement('article');
    bubble.className = 'message-bubble';
    bubble.addEventListener('click', function onBubbleClick(event) {
      if (shouldIgnoreReactionInteraction(event.target)) {
        return;
      }

      const selection = window.getSelection ? window.getSelection().toString() : '';
      if (selection) {
        return;
      }

      toggleReactionPicker(message, stack);
    });

    const head = document.createElement('div');
    head.className = 'message-head';

    const headLeft = document.createElement('div');
    headLeft.className = 'message-head-left';

    const avatar = createAvatarNode(message.senderId, message.senderAvatarUrl);

    const author = document.createElement('p');
    author.className = 'message-author';
    author.textContent = message.senderName || getUserNameById(message.senderId) || 'Пользователь';

    const time = document.createElement('span');
    time.className = 'message-time';
    time.textContent = formatTime(message.createdAt);

    headLeft.append(avatar, author);
    head.append(headLeft, time);
    bubble.append(head);

    if (message.text) {
      const textElement = document.createElement('p');
      textElement.className = 'message-text';
      textElement.textContent = message.text;
      bubble.append(textElement);
    }

    if (Array.isArray(message.attachments) && message.attachments.length > 0) {
      bubble.append(renderMessageAttachments(message.attachments));
    }

    stack.append(bubble);

    const reactionsRow = renderMessageReactions(message);
    if (reactionsRow) {
      stack.append(reactionsRow);
    }

    item.append(stack);
    fragment.append(item);
  });

  elements.messagesList.innerHTML = '';
  elements.messagesList.append(fragment);
}

function renderMessageAttachments(attachments) {
  const list = document.createElement('div');
  list.className = 'attachments-list';

  attachments.forEach(function appendAttachment(attachment) {
    const item = document.createElement('div');
    item.className = 'attachment-item';

    if (attachment.kind === 'image') {
      const image = document.createElement('img');
      image.className = 'attachment-image';
      image.src = attachment.url;
      image.alt = attachment.name || 'image';
      image.loading = 'lazy';
      image.addEventListener('click', function onImageClick(event) {
        event.stopPropagation();
        openMediaModal(attachment);
      });
      image.addEventListener('keydown', function onImageKeydown(event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          openMediaModal(attachment);
        }
      });
      image.tabIndex = 0;
      item.append(image, createAttachmentOpenButton(attachment));
    } else if (attachment.kind === 'video') {
      const video = document.createElement('video');
      video.className = 'attachment-video';
      video.src = attachment.url;
      video.controls = true;
      video.preload = 'metadata';
      video.playsInline = true;
      item.append(video, createAttachmentOpenButton(attachment));
    } else if (attachment.kind === 'audio') {
      const audio = document.createElement('audio');
      audio.className = 'attachment-audio';
      audio.src = attachment.url;
      audio.controls = true;
      item.append(audio);
    } else {
      const fileLink = document.createElement('a');
      fileLink.className = 'attachment-file';
      fileLink.href = attachment.url;
      fileLink.target = '_blank';
      fileLink.rel = 'noopener noreferrer';
      fileLink.textContent = `Скачать: ${attachment.name || 'file'}`;
      item.append(fileLink);
    }

    list.append(item);
  });

  return list;
}

function createAttachmentOpenButton(attachment) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'attachment-open';
  button.textContent = '⤢';
  button.title = 'Открыть на весь экран';
  button.addEventListener('click', function onAttachmentOpen(event) {
    event.stopPropagation();
    openMediaModal(attachment);
  });
  return button;
}

function openMediaModal(attachment) {
  if (!elements.mediaModal || !elements.mediaModalContent) {
    return;
  }

  if (!attachment || !attachment.url) {
    return;
  }

  closeReactionPicker();

  const kind = attachment.kind || detectAttachmentKind(attachment.mimeType || '');

  if (kind !== 'image' && kind !== 'video') {
    return;
  }

  elements.mediaModalContent.innerHTML = '';

  let node = null;

  if (kind === 'image') {
    const image = document.createElement('img');
    image.src = attachment.url;
    image.alt = attachment.name || 'image';
    node = image;
  } else if (kind === 'video') {
    const video = document.createElement('video');
    video.src = attachment.url;
    video.controls = true;
    video.autoplay = true;
    video.playsInline = true;
    node = video;
  }

  if (!node) {
    return;
  }

  elements.mediaModalContent.append(node);
  elements.mediaModal.hidden = false;

  if (elements.mediaModalName) {
    elements.mediaModalName.textContent = attachment.name || 'Медиа';
  }

  if (elements.mediaModalDownload) {
    elements.mediaModalDownload.href = attachment.url;
    if (attachment.name) {
      elements.mediaModalDownload.setAttribute('download', attachment.name);
    } else {
      elements.mediaModalDownload.removeAttribute('download');
    }
  }
}

function closeMediaModal() {
  if (!elements.mediaModal || !elements.mediaModalContent) {
    return;
  }

  elements.mediaModal.hidden = true;
  elements.mediaModalContent.innerHTML = '';

  if (elements.mediaModalName) {
    elements.mediaModalName.textContent = '';
  }

  if (elements.mediaModalDownload) {
    elements.mediaModalDownload.removeAttribute('href');
    elements.mediaModalDownload.removeAttribute('download');
  }
}

// Реакции рендерятся отдельно от сообщения, чтобы можно было обновлять их точечно по socket-событию.
function renderMessageReactions(message) {
  if (!message || !message.id) {
    return null;
  }

  const reactions = getReactionEntries(message.reactions);
  if (reactions.length === 0) {
    return null;
  }

  const row = document.createElement('div');
  row.className = 'message-reaction-row';

  reactions.forEach(function appendReaction(entry) {
    const emoji = entry[0];
    const users = entry[1];
    const reactionButton = document.createElement('button');
    reactionButton.type = 'button';
    reactionButton.className = 'reaction-chip';
    reactionButton.textContent = `${emoji} ${users.length}`;

    if (state.currentUser && users.includes(state.currentUser.id)) {
      reactionButton.classList.add('active');
    }

    reactionButton.addEventListener('click', function onReactionClick(event) {
      event.stopPropagation();
      sendMessageReaction(message.chatId, message.id, emoji);
    });

    row.append(reactionButton);
  });

  return row;
}

function toggleReactionPicker(message, stack) {
  if (!message || !message.id || !stack) {
    return;
  }

  if (state.reactionPickerMessageId === message.id) {
    closeReactionPicker();
    return;
  }

  openReactionPicker(message, stack);
}

function openReactionPicker(message, stack) {
  closeReactionPicker();

  const picker = document.createElement('div');
  picker.className = 'reaction-picker';

  REACTION_EMOJIS.forEach(function appendReactionOption(emoji) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'reaction-picker-item';
    button.textContent = emoji;
    button.addEventListener('click', function onReactionPick(event) {
      event.stopPropagation();
      sendMessageReaction(message.chatId, message.id, emoji);
      closeReactionPicker();
    });
    picker.append(button);
  });

  const reactionRow = stack.querySelector('.message-reaction-row');
  if (reactionRow) {
    stack.insertBefore(picker, reactionRow);
  } else {
    stack.append(picker);
  }

  state.reactionPickerMessageId = message.id;
  state.reactionPickerNode = picker;
}

function closeReactionPicker() {
  if (state.reactionPickerNode && state.reactionPickerNode.parentNode) {
    state.reactionPickerNode.parentNode.removeChild(state.reactionPickerNode);
  }

  state.reactionPickerNode = null;
  state.reactionPickerMessageId = null;
}

async function sendMessageReaction(chatId, messageId, emoji) {
  if (!chatId || !messageId || !emoji) {
    return;
  }

  const response = await emitWithAck('message:reaction', {
    chatId,
    messageId,
    emoji,
  });

  if (!response || !response.ok) {
    showToast(response && response.error ? response.error : 'Не удалось добавить реакцию');
    return;
  }

  applyMessageReactionsUpdate(chatId, messageId, response.reactions);
}

function normalizeMessageReactions(value) {
  const normalized = {};

  if (!value || typeof value !== 'object') {
    return normalized;
  }

  Object.keys(value).forEach(function mapReaction(emoji) {
    if (!REACTION_EMOJI_SET.has(emoji)) {
      return;
    }

    const rawUsers = Array.isArray(value[emoji]) ? value[emoji] : [];
    const seen = new Set();
    const users = [];

    rawUsers.forEach(function addUser(rawUser) {
      const userId = String(rawUser || '').trim();
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

function getReactionEntries(reactions) {
  const normalized = normalizeMessageReactions(reactions);
  const emojis = Object.keys(normalized);

  emojis.sort(function sortReactions(first, second) {
    const countDiff = normalized[second].length - normalized[first].length;
    if (countDiff !== 0) {
      return countDiff;
    }

    return REACTION_EMOJIS.indexOf(first) - REACTION_EMOJIS.indexOf(second);
  });

  return emojis.map(function mapEntry(emoji) {
    return [emoji, normalized[emoji]];
  });
}

function shouldIgnoreReactionInteraction(target) {
  if (!(target instanceof Element)) {
    return false;
  }

  if (target.closest('a, button, input, textarea, select')) {
    return true;
  }

  if (target.closest('audio, video')) {
    return true;
  }

  if (target.closest('.attachment-open')) {
    return true;
  }

  return false;
}

function scrollMessagesToBottom() {
  elements.messageFeed.scrollTop = elements.messageFeed.scrollHeight;
}

function isMessageFeedAtBottom() {
  const threshold = 16;
  return (
    elements.messageFeed.scrollHeight - elements.messageFeed.scrollTop - elements.messageFeed.clientHeight < threshold
  );
}

function setComposerEnabled(isEnabled) {
  if (state.pendingChannelToSubscribe) {
    elements.messageInput.disabled = true;
    elements.attachButton.disabled = true;
    elements.sendButton.disabled = true;
    elements.messageInput.placeholder = 'Подпишись на канал, чтобы открыть сообщения';
    showInlineChannelSubscribeButton();
    return;
  }

  hideInlineChannelSubscribeButton();
  elements.messageInput.disabled = !isEnabled;
  elements.attachButton.disabled = !isEnabled;
  elements.sendButton.disabled = !isEnabled;

  if (isEnabled) {
    elements.messageInput.placeholder = 'Напишите сообщение...';
    return;
  }

  const currentChat = state.currentChatId ? state.chatsById.get(state.currentChatId) : null;

  if (currentChat && currentChat.type === 'channel' && currentChat.canSendMessages === false) {
    elements.messageInput.placeholder = 'В этом канале писать могут только админы';
    return;
  }

  elements.messageInput.placeholder = 'Напишите сообщение...';
}

function showInlineChannelSubscribeButton() {
  elements.composerControls.hidden = true;
  elements.composerPreview.hidden = true;
  elements.channelSubscribeInline.hidden = false;
  elements.channelSubscribeInline.textContent = 'Подписаться';
}

function hideInlineChannelSubscribeButton() {
  elements.composerControls.hidden = false;
  elements.composerPreview.hidden = false;
  elements.channelSubscribeInline.hidden = true;
}

function clearSelectedFiles() {
  state.selectedFiles.forEach(function revokeSelectedFile(item) {
    URL.revokeObjectURL(item.previewUrl);
  });

  state.selectedFiles = [];
  renderSelectedFilesPreview();
}

function getChatPreviewText(chat) {
  if (!chat.lastMessage) {
    return 'Пока нет сообщений';
  }

  const sender = chat.lastMessage.senderName ? `${chat.lastMessage.senderName}: ` : '';
  return `${sender}${chat.lastMessage.text}`;
}

function getTypeLabel(type) {
  if (type === 'private') {
    return 'Личный';
  }

  if (type === 'group') {
    return 'Группа';
  }

  return 'Канал';
}

function buildAttachmentSummary(attachments) {
  if (!attachments.length) {
    return '';
  }

  if (attachments.length === 1) {
    return `Файл: ${attachments[0].name || 'attachment'}`;
  }

  return `Вложения: ${attachments.length}`;
}

// Логика звонка: локальные медиа, подключение к комнате и обмен WebRTC-сигналами.
function handleStartAudioCall() {
  startCall('audio');
}

function handleStartVideoCall() {
  startCall('video');
}

async function startCall(mode) {
  if (!state.currentChatId || !state.currentUser) {
    showToast('Выбери чат для звонка');
    return;
  }

  if (state.activeCall && state.activeCall.chatId === state.currentChatId) {
    showToast('Звонок уже активен в этом чате');
    return;
  }

  const callResponse = await emitWithAck('call:start', {
    chatId: state.currentChatId,
    mode,
  });

  if (!callResponse || !callResponse.ok) {
    showToast(callResponse && callResponse.error ? callResponse.error : 'Не удалось начать звонок');
    return;
  }

  await joinCall(state.currentChatId, mode);
}

async function joinCall(chatId, requestedMode) {
  let actualMode = requestedMode;

  try {
    actualMode = await ensureActiveCallState(chatId, requestedMode);
  } catch (error) {
    showToast(buildMediaAccessErrorMessage(error, requestedMode));
    return;
  }

  const joinResponse = await emitWithAck('call:join', {
    chatId,
    mode: actualMode,
  });

  if (!joinResponse || !joinResponse.ok) {
    showToast(joinResponse && joinResponse.error ? joinResponse.error : 'Не удалось подключиться к звонку');
    await endActiveCallLocally();
    return;
  }

  openCallPanel(chatId, joinResponse.mode || actualMode);

  const participants = Array.isArray(joinResponse.participants) ? joinResponse.participants : [];

  participants.forEach(function rememberParticipant(participant) {
    if (participant && participant.id) {
      state.activeCall.participantNames.set(participant.id, participant.name || 'Участник');
    }
  });

  for (const participant of participants) {
    await ensurePeerConnection(participant.id, true);
  }

  elements.callStatus.textContent = 'Звонок активен';
}

async function ensureActiveCallState(chatId, mode) {
  if (state.activeCall && state.activeCall.chatId === chatId) {
    return state.activeCall.mode;
  }

  if (state.activeCall) {
    await endActiveCallLocally();
  }

  const localStream = await requestLocalMedia(mode);

  state.activeCall = {
    chatId,
    mode,
    localStream,
    peerConnections: new Map(),
    remoteStreams: new Map(),
    participantNames: new Map(),
  };

  elements.localVideo.srcObject = localStream;

  return mode;
}

async function requestLocalMedia(mode) {
  if (!navigator.mediaDevices || typeof navigator.mediaDevices.getUserMedia !== 'function') {
    const error = new Error('MediaDevices API unavailable');
    error.code = 'MEDIA_UNAVAILABLE';
    throw error;
  }

  if (!window.isSecureContext && !isLocalhostHost(location.hostname)) {
    const error = new Error('Insecure context');
    error.code = 'INSECURE_CONTEXT';
    throw error;
  }

  const constraints = {
    audio: {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    },
    video: mode === 'video',
  };

  return navigator.mediaDevices.getUserMedia(constraints);
}

function buildMediaAccessErrorMessage(error, mode) {
  if (error && error.code === 'INSECURE_CONTEXT') {
    return 'Для звонков нужен безопасный адрес: https://... или localhost. По обычному http доступ к микрофону блокируется браузером.';
  }

  if (error && error.code === 'MEDIA_UNAVAILABLE') {
    return 'Браузер не поддерживает доступ к микрофону/камере через MediaDevices API.';
  }

  const name = error && error.name ? error.name : '';

  if (name === 'NotAllowedError' || name === 'SecurityError') {
    return 'Браузер запретил доступ к микрофону/камере. Разреши доступ в настройках сайта.';
  }

  if (name === 'NotFoundError' || name === 'DevicesNotFoundError') {
    return mode === 'video'
      ? 'Не найдены устройства: микрофон или камера.'
      : 'Не найден доступный микрофон.';
  }

  if (name === 'NotReadableError' || name === 'TrackStartError') {
    return 'Микрофон или камера заняты другим приложением.';
  }

  return 'Не удалось получить доступ к микрофону/камере.';
}

function openCallPanel(chatId, mode) {
  const chat = state.chatsById.get(chatId);
  elements.callChatTitle.textContent = chat ? chat.title : 'Звонок';
  elements.callStatus.textContent = mode === 'video' ? 'Видеозвонок' : 'Аудиозвонок';
  elements.callPanel.hidden = false;
}

async function ensurePeerConnection(peerUserId, shouldCreateOffer) {
  if (!state.activeCall || !state.currentUser || peerUserId === state.currentUser.id) {
    return null;
  }

  const existing = state.activeCall.peerConnections.get(peerUserId);

  if (existing) {
    return existing;
  }

  const peerConnection = new RTCPeerConnection({
    iceServers: [
      {
        urls: 'stun:stun.l.google.com:19302',
      },
    ],
  });

  state.activeCall.localStream.getTracks().forEach(function addTrack(track) {
    peerConnection.addTrack(track, state.activeCall.localStream);
  });

  peerConnection.addEventListener('icecandidate', function onIceCandidate(event) {
    if (!event.candidate || !state.activeCall) {
      return;
    }

    state.socket.emit('webrtc:ice-candidate', {
      chatId: state.activeCall.chatId,
      toUserId: peerUserId,
      payload: event.candidate,
    });
  });

  peerConnection.addEventListener('track', function onTrack(event) {
    const [remoteStream] = event.streams;

    if (!remoteStream || !state.activeCall) {
      return;
    }

    state.activeCall.remoteStreams.set(peerUserId, remoteStream);
    renderRemoteStreams();
  });

  peerConnection.addEventListener('connectionstatechange', function onConnectionState() {
    const connectionState = peerConnection.connectionState;

    if (connectionState === 'failed' || connectionState === 'disconnected' || connectionState === 'closed') {
      removePeerConnection(peerUserId);
    }
  });

  state.activeCall.peerConnections.set(peerUserId, peerConnection);

  if (shouldCreateOffer) {
    const offer = await peerConnection.createOffer();
    await peerConnection.setLocalDescription(offer);

    state.socket.emit('webrtc:offer', {
      chatId: state.activeCall.chatId,
      toUserId: peerUserId,
      payload: offer,
    });
  }

  return peerConnection;
}

async function handleWebRtcOfferEvent(eventPayload) {
  if (!eventPayload || !state.activeCall || eventPayload.chatId !== state.activeCall.chatId) {
    return;
  }

  if (eventPayload.fromUserId && eventPayload.fromUserName) {
    state.activeCall.participantNames.set(eventPayload.fromUserId, eventPayload.fromUserName);
  }

  const fromUserId = eventPayload.fromUserId;
  const offer = eventPayload.payload;

  if (!fromUserId || !offer) {
    return;
  }

  const peerConnection = await ensurePeerConnection(fromUserId, false);

  if (!peerConnection) {
    return;
  }

  await peerConnection.setRemoteDescription(new RTCSessionDescription(offer));

  const answer = await peerConnection.createAnswer();
  await peerConnection.setLocalDescription(answer);

  state.socket.emit('webrtc:answer', {
    chatId: state.activeCall.chatId,
    toUserId: fromUserId,
    payload: answer,
  });
}

async function handleWebRtcAnswerEvent(eventPayload) {
  if (!eventPayload || !state.activeCall || eventPayload.chatId !== state.activeCall.chatId) {
    return;
  }

  const fromUserId = eventPayload.fromUserId;
  const answer = eventPayload.payload;

  if (!fromUserId || !answer) {
    return;
  }

  const peerConnection = state.activeCall.peerConnections.get(fromUserId);

  if (!peerConnection) {
    return;
  }

  await peerConnection.setRemoteDescription(new RTCSessionDescription(answer));
}

async function handleWebRtcIceCandidateEvent(eventPayload) {
  if (!eventPayload || !state.activeCall || eventPayload.chatId !== state.activeCall.chatId) {
    return;
  }

  const fromUserId = eventPayload.fromUserId;
  const candidate = eventPayload.payload;

  if (!fromUserId || !candidate) {
    return;
  }

  const peerConnection = state.activeCall.peerConnections.get(fromUserId);

  if (!peerConnection) {
    return;
  }

  try {
    await peerConnection.addIceCandidate(new RTCIceCandidate(candidate));
  } catch (error) {
    console.error('Failed to add ICE candidate:', error);
  }
}

function handleCallStartedEvent(payload) {
  if (!payload || !payload.chatId || !payload.startedBy) {
    return;
  }

  if (state.currentUser && payload.startedBy.id === state.currentUser.id) {
    return;
  }

  showToast(
    `${payload.startedBy.name} начал ${payload.mode === 'video' ? 'видеозвонок' : 'аудиозвонок'} в чате`,
  );
}

function handleIncomingCallEvent(payload) {
  if (!payload || !payload.chatId || !payload.from) {
    return;
  }

  if (state.currentUser && payload.from.id === state.currentUser.id) {
    return;
  }

  state.incomingCall = payload;

  const chat = state.chatsById.get(payload.chatId);
  const chatTitle = chat ? chat.title : 'чат';
  const modeText = payload.mode === 'video' ? 'видеозвонок' : 'аудиозвонок';

  elements.incomingCallText.textContent = `${payload.from.name} приглашает в ${modeText} (${chatTitle})`;
  elements.incomingCall.hidden = false;
}

async function handleAcceptIncomingCall() {
  if (!state.incomingCall) {
    return;
  }

  const callData = state.incomingCall;

  state.incomingCall = null;
  elements.incomingCall.hidden = true;

  await selectChat(callData.chatId);
  await joinCall(callData.chatId, callData.mode || 'audio');
}

function handleDeclineIncomingCall() {
  state.incomingCall = null;
  elements.incomingCall.hidden = true;
}

async function handleCallParticipantJoinedEvent(payload) {
  if (!state.activeCall || !payload || payload.chatId !== state.activeCall.chatId || !payload.participant) {
    return;
  }

  state.activeCall.participantNames.set(payload.participant.id, payload.participant.name || 'Участник');
  elements.callStatus.textContent = `${payload.participant.name || 'Участник'} подключился`;

  await ensurePeerConnection(payload.participant.id, false);
}

function handleCallParticipantLeftEvent(payload) {
  if (!state.activeCall || !payload || payload.chatId !== state.activeCall.chatId || !payload.participant) {
    return;
  }

  removePeerConnection(payload.participant.id);
  elements.callStatus.textContent = `${payload.participant.name || 'Участник'} вышел из звонка`;
}

async function handleCallEndedEvent(payload) {
  if (!payload || !state.activeCall || payload.chatId !== state.activeCall.chatId) {
    return;
  }

  await endActiveCallLocally();
  showToast('Звонок завершён');
}

function removePeerConnection(peerUserId) {
  if (!state.activeCall) {
    return;
  }

  const peerConnection = state.activeCall.peerConnections.get(peerUserId);

  if (peerConnection) {
    peerConnection.close();
  }

  state.activeCall.peerConnections.delete(peerUserId);
  state.activeCall.remoteStreams.delete(peerUserId);
  state.activeCall.participantNames.delete(peerUserId);

  renderRemoteStreams();
}

function renderRemoteStreams() {
  if (!state.activeCall) {
    elements.remoteVideos.innerHTML = '';
    return;
  }

  elements.remoteVideos.innerHTML = '';

  state.activeCall.remoteStreams.forEach(function appendRemoteStream(stream, userId) {
    const card = document.createElement('div');
    card.className = 'remote-video-card';

    const name = document.createElement('p');
    name.textContent = getUserNameById(userId) || 'Участник';

    const hasVideoTrack = stream.getVideoTracks().length > 0;

    if (hasVideoTrack) {
      const video = document.createElement('video');
      video.autoplay = true;
      video.playsInline = true;
      video.srcObject = stream;
      card.append(name, video);
    } else {
      const audio = document.createElement('audio');
      audio.controls = true;
      audio.autoplay = true;
      audio.srcObject = stream;

      const audioText = document.createElement('p');
      audioText.textContent = 'Аудио поток';
      card.append(name, audioText, audio);
    }

    elements.remoteVideos.append(card);
  });
}

async function handleLeaveCallClick() {
  if (!state.activeCall) {
    return;
  }

  await emitWithAck('call:leave', {
    chatId: state.activeCall.chatId,
  });

  await endActiveCallLocally();
}

async function endActiveCallLocally() {
  if (!state.activeCall) {
    return;
  }

  state.activeCall.peerConnections.forEach(function closePeerConnection(peerConnection) {
    peerConnection.close();
  });

  state.activeCall.localStream.getTracks().forEach(function stopTrack(track) {
    track.stop();
  });

  elements.localVideo.srcObject = null;
  elements.remoteVideos.innerHTML = '';
  elements.callPanel.hidden = true;

  state.activeCall = null;
}

function handleBeforeUnload() {
  if (!state.activeCall) {
    return;
  }

  state.socket.emit('call:leave', {
    chatId: state.activeCall.chatId,
  });
}

function updateCallButtonsState() {
  const enabled = Boolean(state.currentUser && state.currentChatId);
  elements.audioCallButton.disabled = !enabled;
  elements.videoCallButton.disabled = !enabled;
}

function updateManageChatButtonState() {
  if (!state.currentUser || !state.currentChatId) {
    elements.chatManageButton.disabled = true;
    return;
  }

  const chat = state.chatsById.get(state.currentChatId);
  const enabled = Boolean(chat && (chat.type === 'group' || chat.type === 'channel') && chat.canManageMembers);
  elements.chatManageButton.disabled = !enabled;
}

// Модалка управления участниками позволяет админам менять роли и исключать участников.
async function handleOpenManageChatModal() {
  if (!state.currentUser || !state.currentChatId) {
    return;
  }

  const chat = state.chatsById.get(state.currentChatId);

  if (!chat || !chat.canManageMembers || (chat.type !== 'group' && chat.type !== 'channel')) {
    showToast('Недостаточно прав для управления этим чатом');
    return;
  }

  elements.manageChatTitle.textContent = `Управление: ${chat.title}`;
  elements.manageChatDescription.textContent =
    chat.type === 'channel'
      ? 'В канале писать могут только админы. Управляй участниками и правами админов.'
      : 'В группе админы могут исключать участников и назначать других админов.';

  elements.manageChatModal.hidden = false;
  await refreshManageChatMembers();
}

function closeManageChatModal() {
  elements.manageChatModal.hidden = true;
  elements.manageChatMembers.innerHTML = '';
}

async function refreshManageChatMembers() {
  if (!state.currentChatId) {
    return;
  }

  const response = await emitWithAck('chat:members', {
    chatId: state.currentChatId,
  });

  if (!response || !response.ok) {
    elements.manageChatMembers.innerHTML =
      '<li class="manage-member-row"><p class="user-search-empty">Не удалось загрузить участников</p></li>';
    return;
  }

  if (response.chat) {
    upsertChat(response.chat);
  }

  renderManageMembersList(Array.isArray(response.members) ? response.members : []);
}

function renderManageMembersList(members) {
  if (members.length === 0) {
    elements.manageChatMembers.innerHTML =
      '<li class="manage-member-row"><p class="user-search-empty">Участники не найдены</p></li>';
    return;
  }

  const chat = state.currentChatId ? state.chatsById.get(state.currentChatId) : null;

  const fragment = document.createDocumentFragment();

  members.forEach(function appendMember(member) {
    const row = document.createElement('li');
    row.className = 'manage-member-row';

    const info = document.createElement('div');
    info.className = 'manage-member-info';

    const name = document.createElement('p');
    name.className = 'manage-member-name';
    name.textContent = member.username || 'Пользователь';

    const meta = document.createElement('p');
    meta.className = 'manage-member-meta';
    const roleParts = [];
    if (member.isCreator) {
      roleParts.push('создатель');
    } else if (member.isAdmin) {
      roleParts.push('админ');
    } else {
      roleParts.push('участник');
    }
    meta.textContent = roleParts.join(' • ');

    info.append(name, meta);

    const actions = document.createElement('div');
    actions.className = 'manage-member-actions';

    if (chat && member.id !== state.currentUser.id && !member.isCreator) {
      const toggleAdminButton = document.createElement('button');
      toggleAdminButton.type = 'button';
      toggleAdminButton.className = 'button button-secondary';
      toggleAdminButton.textContent = member.isAdmin ? 'Снять админа' : 'Сделать админом';
      toggleAdminButton.addEventListener('click', async function onToggleAdmin() {
        const eventName = member.isAdmin ? 'chat:adminRemove' : 'chat:adminAdd';
        const response = await emitWithAck(eventName, {
          chatId: chat.id,
          memberId: member.id,
        });

        if (!response || !response.ok) {
          showToast(response && response.error ? response.error : 'Операция не выполнена');
          return;
        }

        if (response.chat) {
          upsertChat(response.chat);
        }

        await refreshManageChatMembers();
      });

      const removeButton = document.createElement('button');
      removeButton.type = 'button';
      removeButton.className = 'button button-danger';
      removeButton.textContent = 'Исключить';
      removeButton.addEventListener('click', async function onRemoveMember() {
        const response = await emitWithAck('chat:memberRemove', {
          chatId: chat.id,
          memberId: member.id,
        });

        if (!response || !response.ok) {
          showToast(response && response.error ? response.error : 'Не удалось исключить участника');
          return;
        }

        if (response.chat) {
          upsertChat(response.chat);
        }

        await refreshManageChatMembers();
      });

      actions.append(toggleAdminButton, removeButton);
    }

    row.append(info, actions);
    fragment.append(row);
  });

  elements.manageChatMembers.innerHTML = '';
  elements.manageChatMembers.append(fragment);
}

function handleWindowResize() {
  syncMobileLayout();
}

// При любом ресайзе просто возвращаем мобильный сайдбар в закрытое состояние.
function syncMobileLayout() {
  closeCreateMenu();
  elements.appShell.classList.remove('mobile-sidebar-open');
  elements.sidebarBackdrop.hidden = true;
  elements.mobileSidebarToggle.setAttribute('aria-expanded', 'false');
}

function isMobileLayout() {
  return window.matchMedia('(max-width: 860px)').matches;
}

function openMobileSidebar() {
  if (!isMobileLayout()) {
    return;
  }

  elements.appShell.classList.add('mobile-sidebar-open');
  elements.sidebarBackdrop.hidden = false;
  elements.mobileSidebarToggle.setAttribute('aria-expanded', 'true');
}

function closeMobileSidebar() {
  closeCreateMenu();
  elements.appShell.classList.remove('mobile-sidebar-open');
  elements.sidebarBackdrop.hidden = true;
  elements.mobileSidebarToggle.setAttribute('aria-expanded', 'false');
}

function getUserNameById(userId) {
  if (!userId) {
    return '';
  }

  if (state.currentUser && state.currentUser.id === userId) {
    return state.currentUser.username;
  }

  if (state.activeCall && state.activeCall.participantNames.has(userId)) {
    return state.activeCall.participantNames.get(userId);
  }

  if (state.knownNamesById.has(userId)) {
    return state.knownNamesById.get(userId);
  }

  return '';
}

function getUserAvatarById(userId) {
  if (!userId) {
    return '';
  }

  if (state.currentUser && state.currentUser.id === userId) {
    return normalizeAvatarUrl(state.currentUser.avatarUrl);
  }

  if (state.knownAvatarsById.has(userId)) {
    return normalizeAvatarUrl(state.knownAvatarsById.get(userId));
  }

  return '';
}

function createAvatarNode(userId, explicitAvatarUrl) {
  const avatar = document.createElement('div');
  avatar.className = 'message-avatar';

  const safeAvatarUrl = normalizeAvatarUrl(explicitAvatarUrl || getUserAvatarById(userId));

  if (safeAvatarUrl) {
    const image = document.createElement('img');
    image.src = safeAvatarUrl;
    image.alt = getUserNameById(userId) || 'avatar';
    avatar.append(image);
    return avatar;
  }

  avatar.textContent = '👤';
  return avatar;
}

// Нижний набор хелперов нормализует данные, форматирует вывод и оборачивает socket ack в Promise.
function formatTime(isoString) {
  if (!isoString) {
    return '';
  }

  const date = new Date(isoString);

  if (Number.isNaN(date.getTime())) {
    return '';
  }

  return new Intl.DateTimeFormat('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
    day: '2-digit',
    month: '2-digit',
  }).format(date);
}

function detectAttachmentKind(mimeType) {
  const value = String(mimeType || '');

  if (value.startsWith('image/')) {
    return 'image';
  }

  if (value.startsWith('video/')) {
    return 'video';
  }

  if (value.startsWith('audio/')) {
    return 'audio';
  }

  return 'file';
}

function parseUsernamesInput(rawValue) {
  return String(rawValue || '')
    .split(/[\n,;]+/)
    .map(function mapItem(item) {
      return normalizeText(item);
    })
    .filter(Boolean);
}

function isLocalhostHost(hostname) {
  return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '::1';
}

function saveSessionToken(token) {
  const safeToken = String(token || '').trim();

  if (!safeToken) {
    return;
  }

  localStorage.setItem(SESSION_TOKEN_STORAGE_KEY, safeToken);
}

function loadSessionToken() {
  return String(localStorage.getItem(SESSION_TOKEN_STORAGE_KEY) || '').trim();
}

function clearSessionToken() {
  localStorage.removeItem(SESSION_TOKEN_STORAGE_KEY);
}

function normalizeAvatarUrl(value) {
  const raw = String(value || '').trim();

  if (!raw) {
    return '';
  }

  if (raw.startsWith('/uploads/')) {
    return raw;
  }

  if (raw.startsWith('http://') || raw.startsWith('https://')) {
    return raw;
  }

  if (raw.startsWith('blob:')) {
    return raw;
  }

  if (raw.startsWith('data:image/')) {
    return raw;
  }

  return '';
}

function normalizeText(value) {
  return String(value || '').trim();
}

function createLocalId() {
  return `local-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

function showToast(text) {
  elements.toast.textContent = text;
  elements.toast.hidden = false;

  clearTimeout(showToast.timer);

  showToast.timer = setTimeout(function hideToast() {
    elements.toast.hidden = true;
  }, 3200);
}

async function safeParseJson(response) {
  try {
    return await response.json();
  } catch (error) {
    return { ok: false, error: 'Invalid server response' };
  }
}

function emitWithAck(eventName, payload) {
  return new Promise(function createAckPromise(resolve) {
    state.socket.emit(eventName, payload, function resolveAck(response) {
      resolve(response);
    });
  });
}
