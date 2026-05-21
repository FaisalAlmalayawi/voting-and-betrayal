// التصويت والخيانة - Voting and Betrayal
// Node.js + Express + Socket.io server

const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

// Keep-alive endpoint to prevent free-host sleep
app.get('/ping', (_req, res) => res.send('pong'));

// ===== Game State =====
/**
 * Players are keyed by a stable `playerKey` (UUID), NOT socket.id, so a player
 * can refresh / reconnect and resume their seat, score, color, and alive state.
 * `room.hostId` is a playerKey. Each player tracks its current `socketId` and
 * `connected` flag. Disconnected players are kept in the room (offline) until
 * the host kicks them or the game fully ends.
 */
const rooms = {};

const HOST_MIGRATION_MS = 120 * 1000;

const PALETTE = [
  { id: 'red',     name: 'أحمر',   hex: '#ef4444' },
  { id: 'blue',    name: 'أزرق',   hex: '#3b82f6' },
  { id: 'green',   name: 'أخضر',   hex: '#22c55e' },
  { id: 'yellow',  name: 'أصفر',   hex: '#eab308' },
  { id: 'purple',  name: 'بنفسجي', hex: '#a855f7' },
  { id: 'orange',  name: 'برتقالي',hex: '#f97316' },
  { id: 'cyan',    name: 'سماوي',  hex: '#06b6d4' },
  { id: 'magenta', name: 'وردي',   hex: '#ec4899' },
  { id: 'white',   name: 'أبيض',   hex: '#f5f5f5' },
  { id: 'black',   name: 'أسود',   hex: '#111111' },
];

function genCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c;
  do {
    c = '';
    for (let i = 0; i < 4; i++) c += chars[Math.floor(Math.random() * chars.length)];
  } while (rooms[c]);
  return c;
}

function genPlayerKey() {
  return crypto.randomBytes(16).toString('hex');
}

function makeRoom(hostKey) {
  return {
    code: genCode(),
    hostId: hostKey, // playerKey of the host
    phase: 'lobby',
    players: {},     // playerKey -> { id, name, colorId, alive, score, socketId, connected }
    order: [],       // playerKey order for stable listing / migration priority
    settings: {
      targetScore: 20,
      discussionSec: 60,
      votingSec: 45,
      showdownSec: 30,
    },
    round: 0,
    votes: {},
    lastResults: null,
    timer: null,
    timerEnd: 0,
    pendingRetaliationBy: null,
    showdownChoices: {},
    goldCards: [],   // array of 4 numbers (1-10) during showdown
    roundOver: true,
    hostMigrationTimer: null,
  };
}

function publicPlayers(room) {
  return room.order
    .filter(id => room.players[id])
    .map(id => {
      const p = room.players[id];
      return {
        id: p.id,
        name: p.name,
        colorId: p.colorId,
        alive: p.alive,
        connected: p.connected,
        hasName: !!p.name,
        hasColor: !!p.colorId,
      };
    });
}

function alivePlayers(room) {
  return Object.values(room.players).filter(p => p.alive);
}
function activeVoters(room) {
  // Alive AND currently connected — these are the players we wait on for vote/showdown completion.
  return alivePlayers(room).filter(p => p.connected);
}

function publicState(room, forKey) {
  const me = room.players[forKey];
  let showCards = room.phase === 'gameover' || room.phase === 'resolution';
  if (room.phase === 'showdown') {
    const fs = alivePlayers(room);
    if (fs.length === 2 && fs.every(p => room.showdownChoices[p.id])) showCards = true;
  }
  const cardsCount = (room.goldCards || []).length;
  return {
    code: room.code,
    phase: room.phase,
    hostId: room.hostId,
    isHost: room.hostId === forKey,
    players: publicPlayers(room),
    palette: PALETTE.map(c => ({
      ...c,
      taken: Object.values(room.players).some(p => p.colorId === c.id),
    })),
    settings: room.settings,
    round: room.round,
    timerEnd: room.timerEnd,
    me: me ? { id: me.id, name: me.name, colorId: me.colorId, alive: me.alive, score: me.score } : null,
    lastResults: room.lastResults,
    pendingRetaliationBy: room.pendingRetaliationBy,
    goldCards: showCards ? (room.goldCards || []) : [],
    goldCardsCount: cardsCount,
    showdownChosenIds: room.phase === 'showdown' ? Object.keys(room.showdownChoices) : [],
    votedIds: room.phase === 'voting' ? Object.keys(room.votes) : [],
    roundOver: room.roundOver,
  };
}

function broadcastState(room) {
  for (const p of Object.values(room.players)) {
    if (p.connected && p.socketId) {
      io.to(p.socketId).emit('state', publicState(room, p.id));
    }
  }
}

function clearTimer(room) {
  if (room.timer) { clearTimeout(room.timer); room.timer = null; }
  room.timerEnd = 0;
}

function setPhaseTimer(room, seconds, onEnd) {
  clearTimer(room);
  room.timerEnd = Date.now() + seconds * 1000;
  room.timer = setTimeout(() => {
    room.timer = null;
    onEnd();
  }, seconds * 1000);
}

function startDiscussion(room) {
  room.round += 1;
  room.votes = {};
  room.lastResults = null;
  room.pendingRetaliationBy = null;
  room.showdownChoices = {};
  room.roundOver = false;
  for (const p of Object.values(room.players)) p.alive = true;
  room.phase = 'discussion';
  clearTimer(room);
  broadcastState(room);
}

function continueRoundDiscussion(room) {
  room.votes = {};
  room.pendingRetaliationBy = null;
  room.phase = 'discussion';
  clearTimer(room);
  broadcastState(room);
}

function startVoting(room) {
  room.phase = 'voting';
  room.votes = {};
  clearTimer(room);
  broadcastState(room);
}

function resolveVotes(room) {
  clearTimer(room);
  const alive = alivePlayers(room);
  const tally = {};
  for (const v of alive) {
    const vote = room.votes[v.id];
    if (vote && vote !== 'PROTECT') tally[vote] = (tally[vote] || 0) + 1;
  }
  let target = null;
  const entries = Object.entries(tally);
  if (entries.length > 0) {
    const max = Math.max(...entries.map(([, n]) => n));
    const tied = entries.filter(([, n]) => n === max).map(([id]) => id);
    target = tied[Math.floor(Math.random() * tied.length)];
  }
  const protectedByTarget = target && room.votes[target] === 'PROTECT';

  const eliminated = new Set();
  const voteList = alive.map(p => ({ voter: p.id, vote: room.votes[p.id] || null }));

  if (!target) {
    for (const p of alive) if (room.votes[p.id] === 'PROTECT') eliminated.add(p.id);
  } else if (protectedByTarget) {
    for (const p of alive) {
      if (p.id === target) continue;
      const v = room.votes[p.id];
      if (v === 'PROTECT') eliminated.add(p.id);
      else if (!v) eliminated.add(p.id);
      else if (v !== target) eliminated.add(p.id);
    }
  } else {
    eliminated.add(target);
    for (const p of alive) {
      if (p.id === target) continue;
      const v = room.votes[p.id];
      if (v === 'PROTECT') eliminated.add(p.id);
      else if (!v) eliminated.add(p.id);
      else if (v !== target) eliminated.add(p.id);
    }
  }

  for (const id of eliminated) {
    if (room.players[id]) room.players[id].alive = false;
  }

  room.lastResults = {
    round: room.round,
    target: target || null,
    protectedByTarget: !!protectedByTarget,
    votes: voteList,
    eliminated: Array.from(eliminated),
    retaliationEliminated: null,
  };

  if (protectedByTarget) {
    const pool = alive
      .filter(p => p.id !== target && room.votes[p.id] === target && room.players[p.id].alive)
      .map(p => p.id);
    if (pool.length > 0) {
      room.pendingRetaliationBy = target;
      room.lastResults.retaliationPool = pool;
      room.phase = 'retaliation';
      broadcastState(room);
      return;
    }
  }
  proceedAfterResolution(room);
}

function proceedAfterResolution(room) {
  const survivors = alivePlayers(room);
  if (survivors.length === 2) { startShowdown(room); return; }
  if (survivors.length === 1) {
    // Sole survivor takes ALL 4 cards
    const cards = generateGoldCards();
    const total = cards.reduce((s, n) => s + n, 0);
    const winner = survivors[0];
    winner.score += total;
    room.goldCards = cards;
    room.lastResults = room.lastResults || { round: room.round, votes: [], eliminated: [] };
    room.lastResults.soleSurvivor = { id: winner.id, points: total, cards };
    room.roundOver = true;
    const anyReached = Object.values(room.players).some(p => p.score >= room.settings.targetScore);
    if (anyReached) {
      room.phase = 'gameover';
      room.lastResults.leaderboard = Object.values(room.players).map(p => ({ id: p.id, name: p.name, score: p.score }));
    } else {
      room.phase = 'resolution';
    }
    broadcastState(room);
    return;
  }
  if (survivors.length === 0) {
    room.roundOver = true;
    room.phase = 'resolution';
    broadcastState(room);
    return;
  }
  room.roundOver = false;
  room.phase = 'resolution';
  broadcastState(room);
}

function generateWeightedCardValue() {
  const r = Math.random();
  let pool;
  if (r < 0.60) {
    // Common: low points 1-4 (60%)
    pool = [1, 2, 3, 4];
  } else if (r < 0.90) {
    // Uncommon: mid points 5-7 (30%)
    pool = [5, 6, 7];
  } else {
    // Rare: high points 8-10 (10%)
    pool = [8, 9, 10];
  }
  return pool[Math.floor(Math.random() * pool.length)];
}

function generateGoldCards() {
  const cards = [];
  for (let i = 0; i < 4; i++) cards.push(generateWeightedCardValue());
  return cards;
}

function startShowdown(room) {
  room.phase = 'showdown';
  room.goldCards = generateGoldCards();
  room.showdownChoices = {};
  broadcastState(room);
  setPhaseTimer(room, room.settings.showdownSec, () => resolveShowdown(room));
}

// Returns per-card assignment array of length 4: each entry is 'a' | 'b' | 'discard'.
// Frontend uses this to animate each card (by index) flying to the correct player.
function distributeAssignments(ca, cb) {
  const key = ca + '+' + cb;
  switch (key) {
    case 'share+share':         return ['a','a','b','b'];
    case 'steal+share':         return ['a','a','a','a'];
    case 'share+steal':         return ['b','b','b','b'];
    case 'steal+steal':         return ['discard','discard','discard','discard'];
    case 'steal+guarantee':     return ['a','a','a','b'];
    case 'guarantee+steal':     return ['b','b','b','a'];
    case 'share+guarantee':     return ['a','a','a','b'];
    case 'guarantee+share':     return ['b','b','b','a'];
    case 'guarantee+guarantee': return ['a','b','discard','discard'];
    default:                    return ['discard','discard','discard','discard'];
  }
}

function distributeCards(cards, ca, cb) {
  const assignments = distributeAssignments(ca, cb);
  const a = [], b = [];
  assignments.forEach((tgt, i) => {
    if (tgt === 'a') a.push(cards[i]);
    else if (tgt === 'b') b.push(cards[i]);
  });
  return { a, b, assignments };
}

function resolveShowdown(room) {
  clearTimer(room);
  const finalists = alivePlayers(room);
  const [a, b] = finalists;
  const ca = room.showdownChoices[a.id] || 'guarantee';
  const cb = room.showdownChoices[b.id] || 'guarantee';
  const cards = room.goldCards && room.goldCards.length === 4 ? room.goldCards : generateGoldCards();
  room.goldCards = cards;

  const dist = distributeCards(cards, ca, cb);
  const sum = arr => arr.reduce((s, n) => s + n, 0);
  const pa = sum(dist.a);
  const pb = sum(dist.b);
  a.score += pa; b.score += pb;

  room.lastResults = {
    round: room.round,
    showdown: {
      cards,
      assignments: dist.assignments,
      a: { id: a.id, choice: ca, cards: dist.a, points: pa },
      b: { id: b.id, choice: cb, cards: dist.b, points: pb },
    },
  };

  room.roundOver = true;
  const anyReached = Object.values(room.players).some(p => p.score >= room.settings.targetScore);
  if (anyReached) {
    room.phase = 'gameover';
    room.lastResults.leaderboard = Object.values(room.players).map(p => ({ id: p.id, name: p.name, score: p.score }));
    broadcastState(room);
    return;
  }
  room.phase = 'resolution';
  broadcastState(room);
}

// ===== Helpers for socket session =====
function meKey(socket) { return socket.data.playerKey; }
function meRoom(socket) { return rooms[socket.data.roomCode]; }
function mePlayer(socket) {
  const room = meRoom(socket);
  if (!room) return null;
  return room.players[meKey(socket)] || null;
}

function attachSocketToRoom(socket, room, playerKey) {
  socket.data.roomCode = room.code;
  socket.data.playerKey = playerKey;
  socket.join(room.code);
}

// ===== Host migration =====
function scheduleHostMigration(room) {
  if (room.hostMigrationTimer) return;
  room.hostMigrationTimer = setTimeout(() => {
    room.hostMigrationTimer = null;
    const host = room.players[room.hostId];
    if (host && host.connected) return; // came back
    // Find next online player in join order
    const next = room.order
      .map(k => room.players[k])
      .find(p => p && p.connected);
    if (next) {
      room.hostId = next.id;
      broadcastState(room);
    }
  }, HOST_MIGRATION_MS);
}
function cancelHostMigration(room) {
  if (room.hostMigrationTimer) { clearTimeout(room.hostMigrationTimer); room.hostMigrationTimer = null; }
}

// ===== Socket Handlers =====
io.on('connection', (socket) => {
  socket.data.roomCode = null;
  socket.data.playerKey = null;

  socket.on('createRoom', ({ name }, cb) => {
    name = (name || '').trim().slice(0, 20);
    if (!name) return cb && cb({ error: 'الاسم مطلوب' });
    const playerKey = genPlayerKey();
    const room = makeRoom(playerKey);
    rooms[room.code] = room;
    room.players[playerKey] = {
      id: playerKey, name, colorId: null, alive: true, score: 0,
      socketId: socket.id, connected: true,
    };
    room.order.push(playerKey);
    attachSocketToRoom(socket, room, playerKey);
    cb && cb({ code: room.code, playerKey });
    broadcastState(room);
  });

  socket.on('joinRoom', ({ code, name }, cb) => {
    code = (code || '').toUpperCase().trim();
    name = (name || '').trim().slice(0, 20);
    const room = rooms[code];
    if (!room) return cb && cb({ error: 'الغرفة غير موجودة' });
    if (!name) return cb && cb({ error: 'الاسم مطلوب' });
    if (Object.keys(room.players).length >= 12) return cb && cb({ error: 'الغرفة ممتلئة' });
    // Same-name reconnect: if a player with this name exists and is offline, resume them.
    const existing = Object.values(room.players).find(p => p.name === name);
    if (existing) {
      if (existing.connected) return cb && cb({ error: 'الاسم مستخدم بالفعل' });
      existing.socketId = socket.id;
      existing.connected = true;
      attachSocketToRoom(socket, room, existing.id);
      if (room.hostId === existing.id) cancelHostMigration(room);
      cb && cb({ code, playerKey: existing.id, resumed: true });
      // Mid-phase sync: send full current phase state directly to the reconnecting socket
      io.to(socket.id).emit('syncGameState', publicState(room, existing.id));
      broadcastState(room);
      return;
    }
    if (room.phase !== 'lobby') return cb && cb({ error: 'اللعبة بدأت بالفعل' });
    const playerKey = genPlayerKey();
    room.players[playerKey] = {
      id: playerKey, name, colorId: null, alive: true, score: 0,
      socketId: socket.id, connected: true,
    };
    room.order.push(playerKey);
    attachSocketToRoom(socket, room, playerKey);
    cb && cb({ code, playerKey });
    broadcastState(room);
  });

  socket.on('reconnectPlayer', ({ code, playerKey }, cb) => {
    code = (code || '').toUpperCase().trim();
    const room = rooms[code];
    if (!room) return cb && cb({ error: 'الغرفة غير موجودة' });
    const p = room.players[playerKey];
    if (!p) return cb && cb({ error: 'الجلسة منتهية' });
    p.socketId = socket.id;
    p.connected = true;
    attachSocketToRoom(socket, room, playerKey);
    if (room.hostId === playerKey) cancelHostMigration(room);
    cb && cb({ code, playerKey, resumed: true });
    // Mid-phase sync: deliver the active phase, timer, and role-specific data immediately
    io.to(socket.id).emit('syncGameState', publicState(room, playerKey));
    broadcastState(room);
  });

  socket.on('pickColor', ({ colorId }) => {
    const room = meRoom(socket); const me = mePlayer(socket);
    if (!room || !me) return;
    if (!PALETTE.find(c => c.id === colorId)) return;
    const taken = Object.values(room.players).some(p => p.id !== me.id && p.colorId === colorId);
    if (taken) return;
    me.colorId = colorId;
    broadcastState(room);
  });

  socket.on('updateSettings', (settings) => {
    const room = meRoom(socket);
    if (!room || room.hostId !== meKey(socket) || room.phase !== 'lobby') return;
    const s = room.settings;
    if (typeof settings.targetScore === 'number') s.targetScore = Math.max(1, Math.min(100, Math.floor(settings.targetScore)));
    if (typeof settings.discussionSec === 'number') s.discussionSec = Math.max(5, Math.min(600, Math.floor(settings.discussionSec)));
    if (typeof settings.votingSec === 'number') s.votingSec = Math.max(5, Math.min(300, Math.floor(settings.votingSec)));
    if (typeof settings.showdownSec === 'number') s.showdownSec = Math.max(5, Math.min(300, Math.floor(settings.showdownSec)));
    broadcastState(room);
  });

  socket.on('startGame', () => {
    const room = meRoom(socket);
    if (!room || room.hostId !== meKey(socket) || room.phase !== 'lobby') return;
    const players = Object.values(room.players);
    if (players.length < 3) return;
    if (players.some(p => !p.colorId)) return;
    room.round = 0;
    startDiscussion(room);
  });

  socket.on('startDiscussionTimer', () => {
    const room = meRoom(socket);
    if (!room || room.phase !== 'discussion') return;
    if (room.hostId !== meKey(socket)) return;
    if (room.timer) return;
    setPhaseTimer(room, room.settings.discussionSec, () => startVoting(room));
    broadcastState(room);
  });

  socket.on('endDiscussion', () => {
    const room = meRoom(socket);
    if (!room || room.phase !== 'discussion') return;
    if (room.hostId !== meKey(socket)) return;
    startVoting(room);
  });

  socket.on('submitVote', ({ vote }) => {
    const room = meRoom(socket); const me = mePlayer(socket);
    if (!room || room.phase !== 'voting') return;
    if (!me || !me.alive) return;
    if (room.votes[me.id] !== undefined) return;
    if (vote !== 'PROTECT') {
      const target = room.players[vote];
      if (!target || !target.alive || target.id === me.id) return;
    }
    const isFirstVote = Object.keys(room.votes).length === 0;
    room.votes[me.id] = vote;
    if (isFirstVote && !room.timer) {
      setPhaseTimer(room, room.settings.votingSec, () => resolveVotes(room));
    }
    broadcastState(room);
    if (activeVoters(room).every(p => room.votes[p.id] !== undefined)) {
      resolveVotes(room);
    }
  });

  socket.on('retaliate', ({ targetId }) => {
    const room = meRoom(socket);
    if (!room || room.phase !== 'retaliation') return;
    if (room.pendingRetaliationBy !== meKey(socket)) return;
    const pool = room.lastResults && room.lastResults.retaliationPool;
    if (!pool || !pool.includes(targetId)) return;
    if (!room.players[targetId]) return;
    room.players[targetId].alive = false;
    room.lastResults.retaliationEliminated = targetId;
    room.lastResults.eliminated.push(targetId);
    room.pendingRetaliationBy = null;
    proceedAfterResolution(room);
  });

  socket.on('showdownChoice', ({ choice }) => {
    const room = meRoom(socket); const me = mePlayer(socket);
    if (!room || room.phase !== 'showdown') return;
    if (!['share', 'steal', 'guarantee'].includes(choice)) return;
    if (!me || !me.alive) return;
    if (room.showdownChoices[me.id]) return;
    room.showdownChoices[me.id] = choice;
    broadcastState(room);
    const finalists = alivePlayers(room);
    if (finalists.every(p => room.showdownChoices[p.id])) {
      resolveShowdown(room);
    }
  });

  socket.on('nextRound', () => {
    const room = meRoom(socket);
    if (!room || room.hostId !== meKey(socket) || room.phase !== 'resolution') return;
    if (room.roundOver) startDiscussion(room);
    else continueRoundDiscussion(room);
  });

  socket.on('kickPlayer', ({ playerKey }) => {
    const room = meRoom(socket);
    if (!room || room.hostId !== meKey(socket)) return;
    if (!playerKey || playerKey === room.hostId) return;
    const p = room.players[playerKey];
    if (!p) return;
    if (p.connected && p.socketId) {
      io.to(p.socketId).emit('kicked');
      const s = io.sockets.sockets.get(p.socketId);
      if (s) { s.data.roomCode = null; s.data.playerKey = null; s.leave(room.code); }
    }
    delete room.players[playerKey];
    room.order = room.order.filter(k => k !== playerKey);
    // Clean their references from active state
    delete room.votes[playerKey];
    delete room.showdownChoices[playerKey];
    broadcastState(room);
  });

  socket.on('playAgain', () => {
    const room = meRoom(socket);
    if (!room || room.hostId !== meKey(socket) || room.phase !== 'gameover') return;
    // Game ended: drop any offline players permanently.
    for (const k of Object.keys(room.players)) {
      if (!room.players[k].connected) {
        delete room.players[k];
        room.order = room.order.filter(x => x !== k);
      }
    }
    for (const p of Object.values(room.players)) { p.score = 0; p.alive = true; }
    room.round = 0;
    room.phase = 'lobby';
    broadcastState(room);
  });

  socket.on('returnToLobby', () => {
    const room = meRoom(socket);
    if (!room || room.hostId !== meKey(socket)) return;
    // Host can bail to lobby from results or game over screens.
    if (room.phase !== 'resolution' && room.phase !== 'gameover') return;
    clearTimer(room);
    for (const p of Object.values(room.players)) { p.score = 0; p.alive = true; }
    room.round = 0;
    room.votes = {};
    room.showdownChoices = {};
    room.lastResults = null;
    room.goldCards = [];
    room.pendingRetaliationBy = null;
    room.roundOver = true;
    room.phase = 'lobby';
    io.to(room.code).emit('forceLobby');
    broadcastState(room);
  });


  socket.on('ping', (cb) => cb && cb('pong'));

  socket.on('disconnect', () => {
    const code = socket.data.roomCode;
    const key = socket.data.playerKey;
    if (!code || !rooms[code] || !key) return;
    const room = rooms[code];
    const p = room.players[key];
    if (!p) return;
    // Ignore if this socket was already replaced by a newer reconnect.
    if (p.socketId !== socket.id) return;

    if (room.phase === 'lobby') {
      // In lobby, disconnect = leave immediately (no game state to preserve).
      delete room.players[key];
      room.order = room.order.filter(k => k !== key);
      if (Object.keys(room.players).length === 0) {
        clearTimer(room); cancelHostMigration(room);
        delete rooms[code];
        return;
      }
      if (room.hostId === key) {
        const next = room.order.find(k => room.players[k] && room.players[k].connected);
        if (next) room.hostId = next;
      }
      broadcastState(room);
    } else {
      // Mid-game: keep seat, just mark offline.
      p.connected = false;
      p.socketId = null;
      // If they were holding up voting/showdown completion, re-check.
      if (room.phase === 'voting') {
        const active = activeVoters(room);
        if (active.length > 0 && active.every(x => room.votes[x.id] !== undefined)) {
          resolveVotes(room);
        }
      }
      if (room.hostId === key) scheduleHostMigration(room);
      broadcastState(room);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`التصويت والخيانة — server on http://localhost:${PORT}`);
});
