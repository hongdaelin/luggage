/* ============================================================================
   캐리어 보관 — 공용 데이터 계층 (store.js)

   설계 요점
   1) 접수 1건 = Firestore 문서 1개. 예전처럼 전체 목록을 JSON 한 덩어리로 저장하지
      않으므로, 두 기기가 동시에 작업해도 서로의 변경을 덮어쓰지 않습니다.
   2) onSnapshot 실시간 구독. 폴링이 없어도 다른 기기의 변경이 즉시 반영됩니다.
   3) 접수번호/번호표는 날짜별 카운터 문서에 트랜잭션으로 발급합니다.
      손님 여러 명이 같은 순간에 접수해도 번호가 겹치지 않습니다.
   4) 연결이 끊기면 localStorage 사본으로 계속 동작하고, 화면에 상태를 표시합니다.
      (조용히 따로 저장되어 기기끼리 어긋나던 예전 동작을 막기 위한 것입니다)
   ========================================================================== */
(function (global) {
  'use strict';

  var firebaseConfig = {
    apiKey: "AIzaSyAhZIlGyr13I6Gw03fgyDJR6Sz-kwH1_UY",
    authDomain: "hongdae-luggage-lin-ver2.firebaseapp.com",
    projectId: "hongdae-luggage-lin-ver2",
    storageBucket: "hongdae-luggage-lin-ver2.firebasestorage.app",
    messagingSenderId: "106221284397",
    appId: "1:106221284397:web:02387d65e1cec46f3d5ca4"
  };

  // Firestore 컬렉션 이름
  var COL = {
    tickets: 'carrier_tickets',
    reservations: 'carrier_reservations',
    counters: 'carrier_counters',
    config: 'carrier_config',
    stats: 'carrier_stats',
    legacy: 'carrier_data'          // 예전 방식(통짜 JSON) — 데이터 이관용으로만 읽음
  };

  // 오프라인 대비 로컬 사본 키
  var LS = {
    tickets: 'carrier:v2:tickets',
    reservations: 'carrier:v2:reservations',
    config: 'carrier:config'
  };

  var DEFAULT_CONFIG = {
    carrierPricing: { '5h': { 1: 4000, 2: 7000, 3: 9000 }, '1d': { 1: 10000, 2: 17000, 3: 22000 } },
    otherItemPrice: 3000
  };

  var db = null;
  var auth = null;
  var FieldValue = null;
  var online = false;                       // Firestore 사용 가능 여부
  var config = JSON.parse(JSON.stringify(DEFAULT_CONFIG));

  var statusHandlers = [];
  var status = { state: 'connecting', message: '연결 중…' };

  function setStatus(state, message) {
    if (status.state === state && status.message === message) return;
    status = { state: state, message: message };
    statusHandlers.forEach(function (fn) { try { fn(status); } catch (e) { } });
  }

  /* ---------------------------------------------------------------- 로컬 사본 */
  function lsRead(key, fallback) {
    try {
      var raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch (e) { return fallback; }
  }
  function lsWrite(key, value) {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch (e) { }
  }

  /* ------------------------------------------------------------------ 유틸 */
  function todayStr() { return new Date().toLocaleDateString('sv-SE'); }   // YYYY-MM-DD (로컬 기준)
  function dateKeyOf(iso) { return String(iso || '').slice(0, 10); }

  function newId(prefix) {
    return prefix + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
  }

  function calcPrice(carrierCount, otherCount, duration, cfg) {
    cfg = cfg || config;
    var price = 0;
    if (carrierCount > 0) {
      var tierTable = (cfg.carrierPricing || {})[duration] || {};
      if (tierTable[carrierCount] != null) {
        price += tierTable[carrierCount];
      } else {
        var keys = Object.keys(tierTable).map(Number).sort(function (a, b) { return a - b; });
        if (keys.length) {
          var maxKey = keys[keys.length - 1];
          price += Math.round((tierTable[maxKey] / maxKey) * carrierCount);
        }
      }
    }
    if (otherCount > 0) price += otherCount * (cfg.otherItemPrice || 0);
    return price;
  }

  function withTimeout(promise, ms) {
    return Promise.race([
      promise,
      new Promise(function (_, reject) {
        setTimeout(function () { reject(new Error('timeout')); }, ms);
      })
    ]);
  }

  /* ------------------------------------------------------------------ 초기화 */
  // opts.role: 'admin'  → 로그인한 사용자만 데이터를 읽습니다 (관리 페이지)
  //            'public' → 로그인 없이 접수/예약 생성만 합니다 (손님용 페이지)
  function init(opts) {
    opts = opts || {};
    var role = opts.role || 'public';

    // 로컬 사본에 있던 설정을 먼저 반영해 두면, 연결 전에도 요금이 0원으로 보이지 않습니다.
    config = Object.assign({}, DEFAULT_CONFIG, lsRead(LS.config, {}));

    if (typeof firebase === 'undefined') {
      online = false;
      setStatus('offline', '오프라인 — 이 기기에만 저장됩니다');
      console.error('Firebase SDK를 불러오지 못했습니다. 인터넷 연결을 확인하세요.');
      return Promise.resolve({ online: false, role: role });
    }

    try {
      firebase.initializeApp(firebaseConfig);
      db = firebase.firestore();
      FieldValue = firebase.firestore.FieldValue;
      if (firebase.auth) auth = firebase.auth();
      online = true;
    } catch (e) {
      online = false;
      setStatus('offline', '오프라인 — 이 기기에만 저장됩니다');
      console.error('Firebase 초기화 실패', e);
      return Promise.resolve({ online: false, role: role });
    }

    // 네트워크가 잠깐 끊겨도 쓰기가 큐에 쌓였다가 복구 시 전송됩니다.
    try {
      db.enablePersistence({ synchronizeTabs: true }).catch(function () { /* 다중 탭/사생활 보호 모드에서는 무시 */ });
    } catch (e) { }

    setStatus('connecting', '연결 중…');
    // 서버 응답이 아예 없으면 onSnapshot이 한 번도 불리지 않아 '연결 중…'에서 멈춥니다.
    // 그대로 두면 동기화가 안 되는 줄 모른 채 계속 쓰게 되므로, 일정 시간 뒤 경고로 바꿉니다.
    setTimeout(function () {
      if (status.state === 'connecting') {
        setStatus('offline', '서버에 연결하지 못했습니다 — 이 기기에만 저장됩니다');
      }
    }, 10000);
    return Promise.resolve({ online: true, role: role });
  }

  /* -------------------------------------------------------------------- 인증 */
  function onAuth(fn) {
    if (!auth) { fn(null); return function () { }; }
    return auth.onAuthStateChanged(fn);
  }
  function signIn(email, password) {
    if (!auth) return Promise.reject(new Error('인증 모듈이 없습니다'));
    return auth.setPersistence(firebase.auth.Auth.Persistence.LOCAL).then(function () {
      return auth.signInWithEmailAndPassword(email, password);
    });
  }
  function signOut() { return auth ? auth.signOut() : Promise.resolve(); }

  /* -------------------------------------------------------------------- 설정 */
  function getConfig() { return config; }

  function onConfig(fn) {
    if (!online) { fn(config); return function () { }; }
    return db.collection(COL.config).doc('main').onSnapshot(function (snap) {
      if (snap.exists && snap.data()) {
        config = Object.assign({}, DEFAULT_CONFIG, snap.data());
        delete config.updatedAt;
        lsWrite(LS.config, config);
      }
      setStatus('online', snap.metadata.fromCache ? '오프라인 캐시 — 연결 대기 중' : '실시간 동기화 중');
      fn(config);
    }, function (err) {
      console.warn('설정 구독 실패 — 로컬 설정 사용', err);
      setStatus('error', err.code === 'permission-denied'
        ? '권한 없음 — 보안 규칙을 확인하세요'
        : '동기화 오류 — 이 기기에만 저장됩니다');
      fn(config);
    });
  }

  function saveConfig(next) {
    config = Object.assign({}, DEFAULT_CONFIG, next);
    lsWrite(LS.config, config);
    if (!online) return Promise.resolve();
    return db.collection(COL.config).doc('main')
      .set(Object.assign({}, config, { updatedAt: Date.now() }), { merge: false });
  }

  /* ------------------------------------------------------- 번호 발급(트랜잭션) */
  // 날짜별 카운터 문서를 트랜잭션으로 증가시켜, 동시 접수에도 번호가 겹치지 않게 합니다.
  // 날짜가 바뀌면 그 날짜의 카운터가 없으므로 자동으로 1번부터 시작합니다.
  function allocateNumbers(dateKey, carrierCount, otherCount) {
    if (online) {
      var ref = db.collection(COL.counters).doc(dateKey);
      var tx = db.runTransaction(function (t) {
        return t.get(ref).then(function (snap) {
          var cur = (snap.exists && snap.data()) || {};
          var seqBase = cur.seq || 0;
          var cBase = cur.carrierTag || 0;
          var eBase = cur.itemTag || 0;
          var carrierTags = [], otherTags = [], i;
          for (i = 1; i <= carrierCount; i++) carrierTags.push(cBase + i);
          for (i = 1; i <= otherCount; i++) otherTags.push(eBase + i);
          t.set(ref, {
            seq: seqBase + 1,
            carrierTag: cBase + carrierCount,
            itemTag: eBase + otherCount,
            updatedAt: Date.now()
          }, { merge: true });
          return { seq: seqBase + 1, carrierTags: carrierTags, otherTags: otherTags, exact: true };
        });
      });
      return withTimeout(tx, 8000).catch(function (e) {
        console.warn('번호 발급 트랜잭션 실패 — 로컬 계산으로 대체합니다', e);
        return localAllocate(dateKey, carrierCount, otherCount);
      });
    }
    return Promise.resolve(localAllocate(dateKey, carrierCount, otherCount));
  }

  // 오프라인 폴백. 이 기기가 아는 오늘 기록만 보고 계산하므로 번호가 겹칠 수 있어
  // exact:false 로 표시해 둡니다.
  function localAllocate(dateKey, carrierCount, otherCount) {
    var mine = lsRead(LS.tickets, []).filter(function (t) { return dateKeyOf(t.checkedInAt) === dateKey; });
    var seq = 1, cMax = 0, eMax = 0;
    mine.forEach(function (t) {
      if ((t.seq || 0) >= seq) seq = (t.seq || 0) + 1;
      (t.carrierTags || []).forEach(function (v) { if (v > cMax) cMax = v; });
      (t.otherTags || []).forEach(function (v) { if (v > eMax) eMax = v; });
    });
    var carrierTags = [], otherTags = [], i;
    for (i = 1; i <= carrierCount; i++) carrierTags.push(cMax + i);
    for (i = 1; i <= otherCount; i++) otherTags.push(eMax + i);
    return { seq: seq, carrierTags: carrierTags, otherTags: otherTags, exact: false };
  }

  // 관리 페이지 "다음 발급 번호" 표시용 (발급하지 않고 조회만)
  function peekNumbers(dateKey) {
    if (!online) {
      var l = localAllocate(dateKey, 1, 1);
      return Promise.resolve({ seq: l.seq, carrierTag: l.carrierTags[0], itemTag: l.otherTags[0], exact: false });
    }
    return db.collection(COL.counters).doc(dateKey).get().then(function (snap) {
      var cur = (snap.exists && snap.data()) || {};
      return {
        seq: (cur.seq || 0) + 1,
        carrierTag: (cur.carrierTag || 0) + 1,
        itemTag: (cur.itemTag || 0) + 1,
        exact: true
      };
    }).catch(function () {
      var l = localAllocate(dateKey, 1, 1);
      return { seq: l.seq, carrierTag: l.carrierTags[0], itemTag: l.otherTags[0], exact: false };
    });
  }

  /* -------------------------------------------------------------------- 접수 */
  function createTicket(input) {
    var dateKey = todayStr();
    return allocateNumbers(dateKey, input.carrierCount, input.otherCount).then(function (n) {
      var ticket = {
        id: newId('tk'),
        seq: n.seq,
        name: input.name,
        phone: input.phone,
        carrierCount: input.carrierCount,
        otherCount: input.otherCount,
        carrierTags: n.carrierTags,
        otherTags: n.otherTags,
        duration: input.duration,
        price: input.price,
        checkedInAt: new Date().toISOString(),
        dateKey: dateKey,
        status: 'active',
        source: input.source || 'unknown',
        numberingExact: n.exact !== false
      };
      mirrorTicket(ticket);
      if (!online) return ticket;
      return db.collection(COL.tickets).doc(ticket.id).set(ticket).then(function () { return ticket; });
    });
  }

  // 수령 완료. 이미 완료된 건을 다시 눌러도 매출이 두 번 더해지지 않도록 트랜잭션으로 처리합니다.
  function completeTicket(ticket) {
    var completedAt = new Date().toISOString();
    var elapsedMinutes = Math.floor((Date.now() - new Date(ticket.checkedInAt).getTime()) / 60000);

    mirrorTicket(Object.assign({}, ticket, { status: 'done', completedAt: completedAt, elapsedMinutes: elapsedMinutes }));
    if (!online) return Promise.resolve();

    var tRef = db.collection(COL.tickets).doc(ticket.id);
    var sRef = db.collection(COL.stats).doc('totals');
    return db.runTransaction(function (t) {
      return t.get(tRef).then(function (snap) {
        if (!snap.exists) throw new Error('이미 삭제된 접수입니다');
        if (snap.data().status === 'done') return;      // 다른 기기에서 이미 처리됨
        t.update(tRef, { status: 'done', completedAt: completedAt, elapsedMinutes: elapsedMinutes });
        t.set(sRef, {
          revenue: FieldValue.increment(snap.data().price || 0),
          count: FieldValue.increment(1)
        }, { merge: true });
      });
    });
  }

  function deleteTicketsByDate(dateKey) {
    if (!online) {
      var kept = lsRead(LS.tickets, []).filter(function (t) {
        return !(t.status === 'done' && dateKeyOf(t.completedAt) === dateKey);
      });
      lsWrite(LS.tickets, kept);
      return Promise.resolve(0);
    }
    // 자정을 넘겨 수령한 건은 접수일(dateKey)과 완료일이 다르므로, 완료일 기준으로 거릅니다.
    return db.collection(COL.tickets).where('status', '==', 'done').get().then(function (all) {
      var docs = all.docs.filter(function (d) { return dateKeyOf((d.data() || {}).completedAt) === dateKey; });
      if (!docs.length) return 0;
      var revenue = docs.reduce(function (s, d) { return s + ((d.data() || {}).price || 0); }, 0);
      var batch = db.batch();
      docs.forEach(function (d) { batch.delete(d.ref); });
      batch.set(db.collection(COL.stats).doc('totals'), {
        revenue: FieldValue.increment(-revenue),
        count: FieldValue.increment(-docs.length)
      }, { merge: true });
      return batch.commit()
        .then(function () { return resetCounterFor(dateKey); })
        .then(function () { return docs.length; });
    });
  }

  // 그 날짜 기록을 지우면 다음 발급 번호도 그만큼 되돌립니다.
  // (기록을 지워도 예전 번호에 이어서 발급되던 문제를 막기 위한 동작입니다)
  function resetCounterFor(dateKey) {
    return db.collection(COL.tickets).where('dateKey', '==', dateKey).get().then(function (qs) {
      var seq = 0, cMax = 0, eMax = 0;
      qs.docs.forEach(function (d) {
        var t = d.data() || {};
        if ((t.seq || 0) > seq) seq = t.seq || 0;
        (t.carrierTags || []).forEach(function (v) { if (v > cMax) cMax = v; });
        (t.otherTags || []).forEach(function (v) { if (v > eMax) eMax = v; });
      });
      return db.collection(COL.counters).doc(dateKey)
        .set({ seq: seq, carrierTag: cMax, itemTag: eMax, updatedAt: Date.now() }, { merge: true });
    });
  }

  /* ---------------------------------------------------------------- 실시간 구독 */
  function subscribe(query, lsKey, fn) {
    if (!online) {
      fn(lsRead(lsKey, []));
      return function () { };
    }
    return query.onSnapshot(function (qs) {
      var rows = qs.docs.map(function (d) { return d.data(); });
      setStatus('online', qs.metadata.fromCache ? '오프라인 캐시 — 연결 대기 중' : '실시간 동기화 중');
      fn(rows);
    }, function (err) {
      console.error('실시간 구독 실패', err);
      setStatus('error', err.code === 'permission-denied'
        ? '권한 없음 — 보안 규칙을 확인하세요'
        : '동기화 오류 — 이 기기에만 저장됩니다');
      fn(lsRead(lsKey, []));
    });
  }

  function onActiveTickets(fn) {
    return subscribe(db ? db.collection(COL.tickets).where('status', '==', 'active') : null, LS.tickets, function (rows) {
      lsWrite(LS.tickets, rows);
      fn(rows.slice().sort(function (a, b) { return (a.seq || 0) - (b.seq || 0); }));
    });
  }

  function onWaitingReservations(fn) {
    return subscribe(db ? db.collection(COL.reservations).where('status', '==', 'waiting') : null, LS.reservations, function (rows) {
      lsWrite(LS.reservations, rows);
      fn(rows.slice().sort(function (a, b) {
        return String(a.expectedArrival).localeCompare(String(b.expectedArrival));
      }));
    });
  }

  // 완료 내역은 날짜별로 그때그때 조회합니다 (전체를 계속 구독하지 않기 위해).
  function listCompletedByDate(dateKey) {
    if (!online) {
      return Promise.resolve(lsRead(LS.tickets, []).filter(function (t) {
        return t.status === 'done' && dateKeyOf(t.completedAt) === dateKey;
      }));
    }
    return db.collection(COL.tickets).where('status', '==', 'done').get().then(function (qs) {
      return qs.docs.map(function (d) { return d.data(); })
        .filter(function (t) { return dateKeyOf(t.completedAt) === dateKey; })
        .sort(function (a, b) { return String(b.completedAt).localeCompare(String(a.completedAt)); });
    });
  }

  function getAllTimeTotals() {
    if (!online) return Promise.resolve({ revenue: 0, count: 0 });
    return db.collection(COL.stats).doc('totals').get().then(function (snap) {
      var d = (snap.exists && snap.data()) || {};
      return { revenue: d.revenue || 0, count: d.count || 0 };
    }).catch(function () { return { revenue: 0, count: 0 }; });
  }

  /* -------------------------------------------------------------------- 예약 */
  function createReservation(input) {
    var reservation = {
      id: newId('rv'),
      name: input.name,
      phone: input.phone,
      carrierCount: input.carrierCount,
      otherCount: input.otherCount,
      duration: input.duration,
      expectedArrival: input.expectedArrival,
      price: input.price,
      createdAt: new Date().toISOString(),
      status: 'waiting'
    };
    if (!online) {
      var list = lsRead(LS.reservations, []);
      list.push(reservation);
      lsWrite(LS.reservations, list);
      return Promise.resolve(reservation);
    }
    return db.collection(COL.reservations).doc(reservation.id).set(reservation)
      .then(function () { return reservation; });
  }

  function updateReservation(id, patch) {
    if (!online) {
      var list = lsRead(LS.reservations, []).map(function (r) {
        return r.id === id ? Object.assign({}, r, patch) : r;
      });
      lsWrite(LS.reservations, list);
      return Promise.resolve();
    }
    return db.collection(COL.reservations).doc(id).update(patch);
  }

  /* ------------------------------------------------------ 로컬 사본 갱신(보조) */
  function mirrorTicket(ticket) {
    var list = lsRead(LS.tickets, []);
    var i = list.findIndex(function (t) { return t.id === ticket.id; });
    if (i >= 0) list[i] = ticket; else list.push(ticket);
    lsWrite(LS.tickets, list);
  }

  /* ------------------------------------------------- 예전 데이터 이관 (1회성) */
  // 예전 방식: carrier_data/{carrier:tickets|carrier:reservations|carrier:config}
  // 에 전체 목록이 JSON 문자열 한 덩어리로 들어 있었습니다.
  function readLegacy() {
    if (!online) return Promise.resolve(null);
    var get = function (key) {
      return db.collection(COL.legacy).doc(key).get().then(function (s) {
        if (!s.exists || !s.data() || s.data().value == null) return null;
        try { return JSON.parse(s.data().value); } catch (e) { return null; }
      }).catch(function () { return null; });
    };
    return Promise.all([get('carrier:tickets'), get('carrier:reservations'), get('carrier:config')])
      .then(function (r) {
        return { tickets: r[0] || [], reservations: r[1] || [], config: r[2] || null };
      });
  }

  function countNewTickets() {
    if (!online) return Promise.resolve(0);
    return db.collection(COL.tickets).limit(1).get()
      .then(function (qs) { return qs.size; })
      .catch(function () { return 0; });
  }

  function migrateLegacy(legacy) {
    if (!online) return Promise.reject(new Error('연결되어 있지 않습니다'));

    var chunks = [];
    var writes = [];

    (legacy.tickets || []).forEach(function (t) {
      var dateKey = dateKeyOf(t.checkedInAt);
      writes.push({
        ref: db.collection(COL.tickets).doc(t.id || newId('tk')),
        data: Object.assign({}, t, { dateKey: dateKey, source: t.source || 'legacy', numberingExact: true })
      });
    });
    (legacy.reservations || []).forEach(function (r) {
      writes.push({
        ref: db.collection(COL.reservations).doc(r.id || newId('rv')),
        data: r
      });
    });

    // 날짜별 카운터를 기존 기록 기준으로 세워 둡니다.
    var counters = {};
    (legacy.tickets || []).forEach(function (t) {
      var k = dateKeyOf(t.checkedInAt);
      if (!k) return;
      var c = counters[k] || (counters[k] = { seq: 0, carrierTag: 0, itemTag: 0 });
      if ((t.seq || 0) > c.seq) c.seq = t.seq || 0;
      (t.carrierTags || []).forEach(function (v) { if (v > c.carrierTag) c.carrierTag = v; });
      (t.otherTags || []).forEach(function (v) { if (v > c.itemTag) c.itemTag = v; });
    });
    Object.keys(counters).forEach(function (k) {
      writes.push({ ref: db.collection(COL.counters).doc(k), data: Object.assign({ updatedAt: Date.now() }, counters[k]) });
    });

    var done = (legacy.tickets || []).filter(function (t) { return t.status === 'done'; });
    writes.push({
      ref: db.collection(COL.stats).doc('totals'),
      data: {
        revenue: done.reduce(function (s, t) { return s + (t.price || 0); }, 0),
        count: done.length
      }
    });

    if (legacy.config) {
      writes.push({
        ref: db.collection(COL.config).doc('main'),
        data: Object.assign({}, DEFAULT_CONFIG, legacy.config, { updatedAt: Date.now() })
      });
    }

    for (var i = 0; i < writes.length; i += 400) chunks.push(writes.slice(i, i + 400));

    return chunks.reduce(function (p, chunk) {
      return p.then(function () {
        var batch = db.batch();
        chunk.forEach(function (w) { batch.set(w.ref, w.data, { merge: true }); });
        return batch.commit();
      });
    }, Promise.resolve()).then(function () { return writes.length; });
  }

  /* ------------------------------------------------------------ 상태 표시 배지 */
  function onStatus(fn) {
    statusHandlers.push(fn);
    fn(status);
    return function () {
      var i = statusHandlers.indexOf(fn);
      if (i >= 0) statusHandlers.splice(i, 1);
    };
  }

  // 어느 페이지에서든 오른쪽 아래에 연결 상태를 띄워 줍니다.
  function mountStatusBadge() {
    var el = document.createElement('div');
    el.id = 'syncBadge';
    el.style.cssText = 'position:fixed;right:12px;bottom:12px;z-index:9999;font-size:12px;font-weight:700;' +
      'padding:7px 12px;border-radius:20px;border:1px solid #2f3742;background:#1c2129;color:#93a0ad;' +
      'display:flex;align-items:center;gap:7px;font-family:inherit;box-shadow:0 2px 10px rgba(0,0,0,.35);';
    el.innerHTML = '<span class="dot" style="width:7px;height:7px;border-radius:50%;background:#93a0ad;flex-shrink:0;"></span><span class="txt"></span>';
    document.body.appendChild(el);
    var colors = { online: '#4fa98c', connecting: '#c9974a', offline: '#d1594c', error: '#d1594c' };
    onStatus(function (s) {
      el.querySelector('.dot').style.background = colors[s.state] || '#93a0ad';
      el.querySelector('.txt').textContent = s.message;
      el.style.borderColor = (s.state === 'offline' || s.state === 'error') ? '#d1594c' : '#2f3742';
      el.style.color = (s.state === 'offline' || s.state === 'error') ? '#f0b7b1' : '#93a0ad';
    });
    return el;
  }

  global.Store = {
    DEFAULT_CONFIG: DEFAULT_CONFIG,
    init: init,
    onStatus: onStatus,
    setStatus: setStatus,
    mountStatusBadge: mountStatusBadge,
    isOnline: function () { return online; },

    onAuth: onAuth, signIn: signIn, signOut: signOut,

    getConfig: getConfig, onConfig: onConfig, saveConfig: saveConfig,
    calcPrice: calcPrice, todayStr: todayStr,

    createTicket: createTicket, completeTicket: completeTicket,
    deleteTicketsByDate: deleteTicketsByDate,
    onActiveTickets: onActiveTickets, listCompletedByDate: listCompletedByDate,
    getAllTimeTotals: getAllTimeTotals, peekNumbers: peekNumbers,

    createReservation: createReservation, updateReservation: updateReservation,
    onWaitingReservations: onWaitingReservations,

    readLegacy: readLegacy, countNewTickets: countNewTickets, migrateLegacy: migrateLegacy
  };
})(window);
