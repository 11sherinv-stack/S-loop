/* S Loop service worker — ساده و امن: شبکه اول، در صورت قطعی از کش */
const V = 'sloop-v4';
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== V).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const r = e.request;
  if (r.method !== 'GET') return;
  const u = new URL(r.url);
  if (u.origin !== location.origin) return;
  if (/\.mp3$/i.test(u.pathname)) return;           // آهنگ را خود مرورگر (با Range) مدیریت کند
  e.respondWith(
    fetch(r).then(res => {
      if (res && res.ok && res.type === 'basic') { const c = res.clone(); caches.open(V).then(ca => ca.put(r, c)); }
      return res;
    }).catch(() => caches.match(r).then(m => m || (r.mode === 'navigate' ? caches.match('./index.html') : undefined)))
  );
});

/* کلیک روی اعلان → بازکردن/فوکوس‌کردن سایت */
self.addEventListener('notificationclick', e => {
  e.notification.close();
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
    for (const c of list) { if ('focus' in c) return c.focus(); }
    return self.clients.openWindow('./');
  }));
});
