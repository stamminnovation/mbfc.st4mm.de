(() => {
  'use strict';

  const SESSION_KEY = 'riprapt-remote-authenticated-v1';
  const encoder = new TextEncoder();
  let appLoaded = false;

  const $ = (id) => document.getElementById(id);

  function bytesFromHex(hex) {
    if (typeof hex !== 'string' || hex.length % 2 !== 0) return null;
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < bytes.length; i++) {
      const value = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
      if (!Number.isFinite(value)) return null;
      bytes[i] = value;
    }
    return bytes;
  }

  function constantTimeEqual(a, b) {
    if (!a || !b || a.length !== b.length) return false;
    let diff = 0;
    for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
    return diff === 0;
  }

  async function derive(password, config) {
    const salt = bytesFromHex(config.salt);
    if (!salt) throw new Error('Ungültige Authentifizierungskonfiguration.');

    const key = await crypto.subtle.importKey(
      'raw',
      encoder.encode(password),
      'PBKDF2',
      false,
      ['deriveBits']
    );

    const bits = await crypto.subtle.deriveBits(
      {
        name: 'PBKDF2',
        hash: 'SHA-256',
        salt,
        iterations: config.iterations
      },
      key,
      256
    );

    return new Uint8Array(bits);
  }

  async function verify(password) {
    const config = window.RIPRAPT_AUTH_CONFIG;
    if (!config || !config.salt || !config.hash || !config.iterations) {
      throw new Error('Der Seitenzugang ist noch nicht konfiguriert. GitHub-Secret RIPRAPT_REMOTE_PASSWORD setzen und Pages neu deployen.');
    }

    const expected = bytesFromHex(config.hash);
    const actual = await derive(password, config);
    return constantTimeEqual(actual, expected);
  }

  function loadApplication() {
    if (appLoaded) return;
    appLoaded = true;
    const script = document.createElement('script');
    script.src = './app.js';
    script.defer = true;
    script.onerror = () => showError('Die Anwendung konnte nicht geladen werden.');
    document.body.appendChild(script);
  }

  function unlock() {
    sessionStorage.setItem(SESSION_KEY, '1');
    document.body.classList.remove('auth-locked');
    $('auth-screen').classList.add('hidden');
    loadApplication();
  }

  function lock() {
    sessionStorage.removeItem(SESSION_KEY);
    location.reload();
  }

  function showError(message) {
    const node = $('auth-error');
    node.textContent = message;
    node.classList.remove('hidden');
  }

  function clearError() {
    const node = $('auth-error');
    node.textContent = '';
    node.classList.add('hidden');
  }

  $('auth-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    clearError();
    const password = $('auth-password').value;
    const button = event.submitter || event.currentTarget.querySelector('button[type="submit"]');
    button.disabled = true;
    button.textContent = 'Prüfe…';

    try {
      if (await verify(password)) {
        $('auth-password').value = '';
        unlock();
      } else {
        showError('Kennwort ist nicht korrekt.');
        $('auth-password').select();
      }
    } catch (error) {
      showError(error?.message || 'Anmeldung fehlgeschlagen.');
    } finally {
      button.disabled = false;
      button.textContent = 'Anmelden';
    }
  });

  $('logout').addEventListener('click', lock);

  if (sessionStorage.getItem(SESSION_KEY) === '1') unlock();
})();
