const fs = require('fs');
const http = require('http');
const path = require('path');
const { Readable } = require('stream');

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');
const REQUEST_TIMEOUT_MS = Number(process.env.MUSIC_API_TIMEOUT_MS || 12000);
const UA = process.env.MUSIC_API_UA ||
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';

const JOOX_TOKEN = process.env.JOOX_TOKEN || 'f84ao9lMF_q7husBWRfgUw';
const JOOX_BR = process.env.JOOX_BR || '4';

const SOURCE_META = {
  netease: { id: 'netease', name: '网易云', accent: '#ef4444' },
  qq: { id: 'qq', name: 'QQ音乐', accent: '#06b6d4' },
  kuwo: { id: 'kuwo', name: '酷我', accent: '#a855f7' },
  joox: { id: 'joox', name: 'JOOX', accent: '#22c55e' },
};

const trackCache = new Map();
const qqMetaCache = new Map();
const neteaseLoginSession = {
  cookie: '',
  profile: null,
  account: null,
  updatedAt: 0,
};

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml; charset=utf-8',
  '.ico': 'image/x-icon',
};

const JOOX_KEYS = {
  songId: '\u6b4c\u66f2ID',
  songName: '\u6b4c\u66f2\u540d\u79f0',
  artist: '\u6b4c\u624b',
  album: '\u4e13\u8f91',
  lyric: '\u6b4c\u8bcd\u5185\u5bb9',
  playLinks: '\u64ad\u653e\u94fe\u63a5',
};

function sendJson(res, data, status = 200) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(JSON.stringify(data));
}

function sendError(res, status, message, extra) {
  sendJson(res, { ok: false, error: message, ...(extra || {}) }, status);
}

function clampInt(value, min, max, fallback) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function parseSources(raw) {
  const all = Object.keys(SOURCE_META);
  if (!raw) return all;
  const picked = String(raw)
    .split(',')
    .map(item => item.trim().toLowerCase())
    .filter(item => SOURCE_META[item]);
  return picked.length ? Array.from(new Set(picked)) : all;
}

function jsonFromPossiblyWrappedText(text) {
  const clean = String(text || '').trim().replace(/^[^(]*\(([\s\S]*)\);?$/, '$1');
  return JSON.parse(clean);
}

async function fetchText(targetUrl, opts = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), opts.timeoutMs || REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(targetUrl, {
      method: opts.method || 'GET',
      headers: {
        'User-Agent': UA,
        Accept: opts.accept || '*/*',
        ...(opts.headers || {}),
      },
      body: opts.body,
      redirect: 'follow',
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      const err = new Error(`HTTP ${response.status} from ${targetUrl}`);
      err.status = response.status;
      err.body = text.slice(0, 600);
      throw err;
    }
    return text;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJson(targetUrl, opts = {}) {
  const text = await fetchText(targetUrl, { ...opts, accept: 'application/json,text/plain,*/*' });
  return jsonFromPossiblyWrappedText(text);
}

async function fetchJsonWithMeta(targetUrl, opts = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), opts.timeoutMs || REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(targetUrl, {
      method: opts.method || 'GET',
      headers: {
        'User-Agent': UA,
        Accept: opts.accept || 'application/json,text/plain,*/*',
        ...(opts.headers || {}),
      },
      body: opts.body,
      redirect: 'follow',
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      const err = new Error(`HTTP ${response.status} from ${targetUrl}`);
      err.status = response.status;
      err.body = text.slice(0, 600);
      throw err;
    }
    return { json: jsonFromPossiblyWrappedText(text || '{}'), text, headers: response.headers };
  } finally {
    clearTimeout(timer);
  }
}

function splitSetCookieHeader(raw) {
  if (!raw) return [];
  return String(raw).split(/,(?=\s*[^;,\s]+=)/).map(item => item.trim()).filter(Boolean);
}

function mergeCookieHeader(existing, setCookieRaw) {
  const jar = new Map();
  String(existing || '').split(';').forEach(part => {
    const index = part.indexOf('=');
    if (index > 0) jar.set(part.slice(0, index).trim(), part.slice(index + 1).trim());
  });
  splitSetCookieHeader(setCookieRaw).forEach(cookie => {
    const first = cookie.split(';')[0] || '';
    const index = first.indexOf('=');
    if (index > 0) jar.set(first.slice(0, index).trim(), first.slice(index + 1).trim());
  });
  return Array.from(jar.entries()).map(([key, value]) => `${key}=${value}`).join('; ');
}

function hasCookieName(cookie, name) {
  const target = String(name || '').toLowerCase();
  return String(cookie || '').split(';').some(part => {
    const index = part.indexOf('=');
    return index > 0 && part.slice(0, index).trim().toLowerCase() === target;
  });
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  const text = Buffer.concat(chunks).toString('utf8').trim();
  if (!text) return {};
  return JSON.parse(text);
}

function pickQueryParam(rawUrl, key) {
  if (!rawUrl) return '';
  try {
    return new URL(rawUrl, 'https://local.invalid/').searchParams.get(key) || '';
  } catch (e) {
    const match = String(rawUrl).match(new RegExp(`[?&]${key}=([^&]+)`));
    return match ? decodeURIComponent(match[1]) : '';
  }
}

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function encodeSquareId(source, payload) {
  const body = Buffer.from(JSON.stringify(payload || {}), 'utf8').toString('base64url');
  return `${source}:${body}`;
}

function decodeSquareId(raw) {
  const text = String(raw || '');
  const match = text.match(/^(netease|kuwo|joox):(.+)$/);
  if (!match) return { source: 'netease', payload: { id: text }, encoded: text };
  try {
    return {
      source: match[1],
      payload: JSON.parse(Buffer.from(match[2], 'base64url').toString('utf8')),
      encoded: text,
    };
  } catch (e) {
    return { source: match[1], payload: { id: match[2] }, encoded: text };
  }
}

function buildTrack(source, data) {
  const sourceMeta = SOURCE_META[source] || { id: source, name: source };
  const id = String(data.id || data.mid || data.songMid || data.uid || '').trim();
  const uid = data.uid || `${source}-${id || data.index || Math.random().toString(36).slice(2)}`;
  return {
    uid,
    provider: source,
    source,
    sourceName: sourceMeta.name,
    type: source === 'qq' ? 'qq' : 'song',
    id,
    mid: String(data.mid || data.songMid || '').trim(),
    songmid: String(data.mid || data.songMid || '').trim(),
    mediaMid: String(data.mediaMid || '').trim(),
    index: data.index || 0,
    keyword: data.keyword || '',
    name: normalizeWhitespace(data.name || data.title),
    artist: normalizeWhitespace(data.artist),
    album: normalizeWhitespace(data.album),
    cover: data.cover || '',
    duration: Number(data.duration || 0) || 0,
    audioUrl: data.audioUrl || '',
    lrc: data.lrc || '',
    lrcUrl: data.lrcUrl || '',
    pageUrl: data.pageUrl || '',
    quality: data.quality || '',
    qualityLabel: data.qualityLabel || '',
    pay: data.pay || '',
    fee: data.fee || 0,
    needsDetail: data.needsDetail !== false,
  };
}

function rememberTrack(track) {
  if (!track) return track;
  const keys = [
    track.uid,
    track.id,
    `${track.source}:${track.id}`,
    track.mid ? `${track.source}:${track.mid}` : '',
    track.mid,
  ].filter(Boolean);
  keys.forEach(key => trackCache.set(String(key), track));
  return track;
}

function rememberTracks(tracks) {
  (tracks || []).forEach(rememberTrack);
  return tracks || [];
}

function cachedTrack(source, idOrMid) {
  const raw = String(idOrMid || '');
  return trackCache.get(`${source}:${raw}`) || trackCache.get(raw) || null;
}

function interleave(groups, limit) {
  const lists = groups.map(group => group.slice());
  const out = [];
  let cursor = 0;
  while (out.length < limit && lists.some(list => list.length)) {
    const list = lists[cursor % lists.length];
    if (list && list.length) out.push(list.shift());
    cursor += 1;
  }
  return out;
}

async function mapLimited(items, concurrency, mapper) {
  const out = new Array(items.length);
  let next = 0;
  const workerCount = Math.max(1, Math.min(concurrency, items.length));
  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (next < items.length) {
      const index = next;
      next += 1;
      out[index] = await mapper(items[index], index);
    }
  }));
  return out;
}

function inferQualityFromUrl(url, fallback) {
  const text = `${fallback || ''} ${url || ''}`.toLowerCase();
  if (/flac|lossless|hi[-_ ]?res|sq|ape|atmos/.test(text)) return { quality: 'lossless', qualityLabel: 'LOSSLESS' };
  const kbps = text.match(/(?:^|[^0-9])(320|256|192|128)k?(?:[^0-9]|$)/);
  if (kbps) return { quality: `${kbps[1]}k`, qualityLabel: `${kbps[1]}K` };
  if (/hq|high/.test(text)) return { quality: 'hq', qualityLabel: 'HQ' };
  return { quality: '', qualityLabel: '' };
}

function qqAlbumCoverUrl(albumMid, size = 300) {
  const mid = String(albumMid || '').trim();
  if (!mid) return '';
  return `https://y.qq.com/music/photo_new/T002R${size}x${size}M000${mid}.jpg?max_age=2592000`;
}

function readQQTrackMetaFromDetail(json, fallbackMid) {
  const track = json && json.songinfo && json.songinfo.data && json.songinfo.data.track_info;
  if (!track || typeof track !== 'object') return {};
  const album = track.album || {};
  const file = track.file || {};
  const singers = Array.isArray(track.singer)
    ? track.singer.map(item => item && (item.name || item.title)).filter(Boolean).join('/')
    : '';
  const albumMid = album.mid || '';
  const mid = track.mid || fallbackMid || '';
  return {
    qqId: track.id || 0,
    mid,
    name: track.name || track.title || '',
    artist: singers,
    album: album.name || album.title || '',
    albumMid,
    cover: qqAlbumCoverUrl(albumMid),
    duration: Number(track.interval || 0) || 0,
    mediaMid: file.media_mid || '',
    pageUrl: mid ? `https://y.qq.com/n/ryqq/songDetail/${mid}` : '',
  };
}

async function fetchQQTrackMeta(mid) {
  const key = String(mid || '').trim();
  if (!key) return {};
  if (qqMetaCache.has(key)) return qqMetaCache.get(key);

  try {
    const body = JSON.stringify({
      comm: { ct: 24, cv: 0 },
      songinfo: {
        method: 'get_song_detail_yqq',
        module: 'music.pf_song_detail_svr',
        param: { song_mid: key },
      },
    });
    const json = await fetchJson('https://u.y.qq.com/cgi-bin/musicu.fcg', {
      method: 'POST',
      body,
      timeoutMs: 7000,
      headers: {
        'Content-Type': 'application/json',
        Referer: 'https://y.qq.com/',
        Origin: 'https://y.qq.com',
      },
    });
    const meta = readQQTrackMetaFromDetail(json, key);
    qqMetaCache.set(key, meta);
    return meta;
  } catch (e) {
    qqMetaCache.set(key, {});
    return {};
  }
}

function extractLyricText(raw) {
  if (!raw) return '';
  const text = String(raw).trim();
  if (!text) return '';
  try {
    const json = jsonFromPossiblyWrappedText(text);
    if (typeof json === 'string') return json;
    return json.lrc || json.lyric ||
      (json.data && (json.data.lrc || json.data.lyric || (typeof json.data === 'string' ? json.data : ''))) ||
      (json.lrc && json.lrc.lyric) ||
      '';
  } catch (e) {
    return text;
  }
}

async function searchNetease(keyword, limit) {
  const api = `https://api.qijieya.cn/meting/?type=search&id=${encodeURIComponent(keyword)}&limit=${limit}&server=netease`;
  const json = await fetchJson(api);
  if (!Array.isArray(json)) return [];
  return json.slice(0, limit).map((item, index) => {
    const nativeId = item.id || pickQueryParam(item.url, 'id') || `${keyword}-${index + 1}`;
    const id = encodeSquareId('netease', { id: nativeId });
    const quality = inferQualityFromUrl(item.url);
    return buildTrack('netease', {
      uid: `netease-${nativeId}`,
      id,
      nativeId,
      index: index + 1,
      keyword,
      name: item.name,
      artist: item.artist,
      cover: item.pic,
      audioUrl: item.url,
      lrcUrl: item.lrc,
      ...quality,
    });
  });
}

function neteaseHeaders(extra = {}) {
  return {
    Referer: 'https://music.163.com/',
    Origin: 'https://music.163.com',
    Cookie: [
      'os=pc',
      'appver=2.9.7',
      neteaseLoginSession.cookie,
    ].filter(Boolean).join('; '),
    ...extra,
  };
}

function neteaseLoginPayload(extra = {}) {
  const profile = neteaseLoginSession.profile || {};
  const account = neteaseLoginSession.account || {};
  const vipType = Number(profile.vipType || account.vipType || 0) || 0;
  const userId = profile.userId || account.id || account.userId || '';
  const loggedIn = hasCookieName(neteaseLoginSession.cookie, 'MUSIC_U');
  return {
    provider: 'netease',
    loggedIn,
    hasCookie: loggedIn,
    userId,
    nickname: profile.nickname || (userId ? `网易云用户 ${userId}` : ''),
    avatar: profile.avatarUrl || '',
    vipType,
    vipLevel: vipType > 10 ? 'svip' : (vipType > 0 ? 'vip' : 'none'),
    isVip: vipType > 0,
    isSvip: vipType > 10,
    updatedAt: neteaseLoginSession.updatedAt || 0,
    ...extra,
  };
}

async function refreshNeteaseProfile() {
  if (!hasCookieName(neteaseLoginSession.cookie, 'MUSIC_U')) return neteaseLoginPayload();
  try {
    const { json, headers } = await fetchJsonWithMeta('https://music.163.com/api/nuser/account/get', {
      headers: neteaseHeaders(),
      timeoutMs: 8000,
    });
    const nextCookie = headers.get('set-cookie');
    if (nextCookie) neteaseLoginSession.cookie = mergeCookieHeader(neteaseLoginSession.cookie, nextCookie);
    if (json && json.code === 200) {
      neteaseLoginSession.profile = json.profile || neteaseLoginSession.profile;
      neteaseLoginSession.account = json.account || neteaseLoginSession.account;
      neteaseLoginSession.updatedAt = Date.now();
    }
  } catch (e) {
    // Keep the saved cookie; the next status check can try again.
  }
  return neteaseLoginPayload();
}

async function createNeteaseQrKey() {
  const api = `https://music.163.com/api/login/qrcode/unikey?type=1&timestamp=${Date.now()}`;
  const { json, headers } = await fetchJsonWithMeta(api, {
    headers: neteaseHeaders(),
    timeoutMs: 8000,
  });
  const nextCookie = headers.get('set-cookie');
  if (nextCookie) neteaseLoginSession.cookie = mergeCookieHeader(neteaseLoginSession.cookie, nextCookie);
  const key = json && (json.unikey || (json.data && json.data.unikey) || json.key);
  if (!key) throw new Error((json && (json.message || json.msg)) || '获取 key 失败');
  return { ok: true, code: 200, key, unikey: key };
}

function createNeteaseQrImage(key) {
  const loginUrl = `https://music.163.com/login?codekey=${encodeURIComponent(key)}`;
  const img = `https://api.qrserver.com/v1/create-qr-code/?size=240x240&margin=1&data=${encodeURIComponent(loginUrl)}`;
  return { ok: true, code: 200, key, qrurl: loginUrl, url: loginUrl, img };
}

async function checkNeteaseQrLogin(key) {
  const api = `https://music.163.com/api/login/qrcode/client/login?key=${encodeURIComponent(key)}&type=1&timestamp=${Date.now()}`;
  const { json, headers } = await fetchJsonWithMeta(api, {
    headers: neteaseHeaders(),
    timeoutMs: 8000,
  });
  const nextCookie = headers.get('set-cookie');
  if (nextCookie) neteaseLoginSession.cookie = mergeCookieHeader(neteaseLoginSession.cookie, nextCookie);
  if (json && json.code === 803) {
    await refreshNeteaseProfile();
    return { ...json, ...neteaseLoginPayload({ code: 803 }) };
  }
  return { loggedIn: false, ...(json || {}) };
}

async function loginNeteaseWithCookie(cookie) {
  neteaseLoginSession.cookie = mergeCookieHeader('', cookie);
  neteaseLoginSession.profile = null;
  neteaseLoginSession.account = null;
  neteaseLoginSession.updatedAt = Date.now();
  return refreshNeteaseProfile();
}

async function searchQQ(keyword, limit) {
  const api = `https://tang.api.s01s.cn/music_open_api.php?msg=${encodeURIComponent(keyword)}&type=json`;
  const json = await fetchJson(api);
  const data = Array.isArray(json) ? json : (Array.isArray(json && json.data) ? json.data : []);
  const tracks = data.slice(0, limit).map((item, index) => {
    const mid = item.song_mid || item.songmid || item.mid || item.id || '';
    return buildTrack('qq', {
      uid: `qq-${mid || index + 1}`,
      id: mid,
      mid,
      index: index + 1,
      keyword,
      name: item.song_title || item.song_name || item.name,
      artist: item.singer_name || item.singer || item.artist,
      album: item.album_name || item.album_title || '',
      cover: item.album_pic || item.singer_pic || '',
      pay: item.pay || '',
    });
  }).filter(track => track.id && track.name);
  return mapLimited(tracks, 5, async track => {
    const meta = await fetchQQTrackMeta(track.mid || track.id);
    return buildTrack('qq', {
      ...track,
      name: track.name || meta.name,
      artist: track.artist || meta.artist,
      album: track.album || meta.album,
      cover: track.cover || meta.cover,
      duration: track.duration || meta.duration,
      mediaMid: track.mediaMid || meta.mediaMid,
      pageUrl: track.pageUrl || meta.pageUrl,
    });
  });
}

async function searchKuwo(keyword, limit) {
  const api = `https://kw-api.cenguigui.cn/?name=${encodeURIComponent(keyword)}&page=1&limit=${limit}`;
  const json = await fetchJson(api);
  const data = json && json.code === 200 && Array.isArray(json.data) ? json.data : [];
  return data.slice(0, limit).map((item, index) => buildTrack('kuwo', {
    uid: `kuwo-${item.rid || index + 1}`,
    id: encodeSquareId('kuwo', { id: item.rid }),
    nativeId: item.rid,
    index: index + 1,
    keyword,
    name: item.name,
    artist: item.artist,
    album: item.album,
    cover: item.pic,
  })).filter(track => track.id && track.name);
}

async function searchJoox(keyword, limit) {
  const api = `https://apicx.asia/api/joox_music?msg=${encodeURIComponent(keyword)}&token=${encodeURIComponent(JOOX_TOKEN)}&br=${encodeURIComponent(JOOX_BR)}`;
  const json = await fetchJson(api);
  const songs = json && json.code === 200 && json.data && Array.isArray(json.data.songs) ? json.data.songs : [];
  return songs.slice(0, limit).map((item, index) => {
    const mid = item.songmid || item.song_mid || '';
    const nativeId = item[JOOX_KEYS.songId] || item.songid || mid || `${index + 1}`;
    const id = encodeSquareId('joox', { id: nativeId, mid, keyword, index: index + 1 });
    return buildTrack('joox', {
      uid: `joox-${mid || nativeId}`,
      id,
      nativeId,
      mid,
      index: index + 1,
      keyword,
      name: item[JOOX_KEYS.songName] || item.song_name || item.name,
      artist: item[JOOX_KEYS.artist] || item.artist || item.singer,
      album: item[JOOX_KEYS.album] || item.album,
      lrc: item[JOOX_KEYS.lyric] || item.lyric || '',
    });
  }).filter(track => track.name);
}

const searchers = { netease: searchNetease, qq: searchQQ, kuwo: searchKuwo, joox: searchJoox };

async function searchAll(keyword, sources, perSourceLimit, totalLimit) {
  const settled = await Promise.allSettled(sources.map(async source => ({
    source,
    songs: await searchers[source](keyword, perSourceLimit),
  })));

  const groups = [];
  const errors = [];
  settled.forEach((result, index) => {
    const source = sources[index];
    if (result.status === 'fulfilled') {
      groups.push(result.value.songs);
    } else {
      errors.push({ source, message: result.reason && result.reason.message || String(result.reason) });
      groups.push([]);
    }
  });

  return {
    songs: rememberTracks(interleave(groups, totalLimit)),
    sources: sources.map((source, index) => ({
      ...SOURCE_META[source],
      count: groups[index] ? groups[index].length : 0,
      error: errors.find(item => item.source === source) || null,
    })),
    errors,
  };
}

function pickBestQQUrl(data) {
  const candidates = [
    ['song_play_url_sq', 'LOSSLESS', 'SQ'],
    ['song_play_url_pq', 'LOSSLESS', 'PQ'],
    ['song_play_url_accom', 'HQ', 'ACCOM'],
    ['song_play_url_hq', 'HQ', 'HQ'],
    ['song_play_url_standard', 'STD', 'STD'],
    ['song_play_url_fq', 'LOW', 'FQ'],
    ['song_play_url', '', ''],
  ];
  for (const [key, label, text] of candidates) {
    if (data && data[key]) {
      const inferred = inferQualityFromUrl(data[key], text || label);
      return {
        audioUrl: data[key],
        quality: inferred.quality,
        qualityLabel: inferred.qualityLabel || label,
        qualityText: text,
      };
    }
  }
  return { audioUrl: '', quality: '', qualityLabel: '', qualityText: '' };
}

async function detailNetease(params) {
  const decoded = decodeSquareId(params.id || '');
  const id = params.nativeId || decoded.payload.id || pickQueryParam(params.audioUrl, 'id');
  let audioUrl = params.audioUrl || '';
  let lrc = params.lrc || '';
  const lrcUrl = params.lrcUrl || (id ? `https://api.qijieya.cn/meting/?server=netease&type=lrc&id=${encodeURIComponent(id)}` : '');

  if (!audioUrl && id) {
    audioUrl = `https://api.qijieya.cn/meting/?server=netease&type=url&id=${encodeURIComponent(id)}`;
  }
  if (!lrc && lrcUrl) {
    try {
      lrc = extractLyricText(await fetchText(lrcUrl));
    } catch (e) {
      lrc = '';
    }
  }
  const quality = inferQualityFromUrl(audioUrl);
  return buildTrack('netease', {
    ...params,
    id,
    audioUrl,
    lrc,
    lrcUrl,
    needsDetail: false,
    ...quality,
  });
}

async function detailQQ(params) {
  const mid = params.mid || params.id;
  if (!mid) throw new Error('Missing QQ song mid');
  const cached = cachedTrack('qq', mid) || {};
  params = { ...cached, ...params, mid };
  const meta = await fetchQQTrackMeta(mid);
  const keyword = params.keyword || `${params.name || ''} ${params.artist || ''}`.trim() || mid;
  const api = `https://tang.api.s01s.cn/music_open_api.php?msg=${encodeURIComponent(keyword || mid)}&type=json&mid=${encodeURIComponent(mid)}`;
  const data = await fetchJson(api);
  if (!data || typeof data !== 'object') throw new Error('Invalid QQ detail response');
  const best = pickBestQQUrl(data);
  return buildTrack('qq', {
    ...params,
    id: data.song_mid || mid,
    mid: data.song_mid || mid,
    name: data.song_title || data.song_name || params.name,
    artist: data.singer_name || params.artist,
    album: data.album_name || data.album_title || params.album || meta.album,
    cover: data.album_pic || data.singer_pic || params.cover || meta.cover,
    duration: params.duration || meta.duration,
    mediaMid: data.media_mid || data.mediaMid || params.mediaMid || meta.mediaMid,
    pageUrl: data.song_h5_url || params.pageUrl || meta.pageUrl,
    audioUrl: best.audioUrl,
    lrc: data.song_lyric || data.lyric || params.lrc,
    quality: best.quality,
    qualityLabel: best.qualityLabel,
    pay: data.vip ? `VIP:${data.vip}` : params.pay,
    needsDetail: false,
  });
}

async function detailKuwo(params) {
  const decoded = decodeSquareId(params.id || '');
  const id = params.nativeId || decoded.payload.id || params.id;
  if (!id) throw new Error('Missing Kuwo song id');
  const api = `https://kw-api.cenguigui.cn/?id=${encodeURIComponent(id)}&type=song&level=zp&format=json`;
  const json = await fetchJson(api);
  if (!json || json.code !== 200 || !json.data) throw new Error('Invalid Kuwo detail response');
  const data = json.data;
  const quality = inferQualityFromUrl(data.url);
  return buildTrack('kuwo', {
    ...params,
    id,
    name: data.name || params.name,
    artist: data.artist || params.artist,
    album: data.album || params.album,
    cover: data.pic || params.cover,
    audioUrl: data.url || params.audioUrl,
    lrc: data.lyric || params.lrc,
    needsDetail: false,
    ...quality,
  });
}

function pickBestJooxUrl(links) {
  const order = [
    ['Atmos\u5168\u666f\u58f0', 'LOSSLESS'],
    ['\u65e0\u635fFLAC', 'LOSSLESS'],
    ['Hi-Res\u65e0\u635f', 'LOSSLESS'],
    ['\u6bcd\u5e26\u65e0\u635f', 'LOSSLESS'],
    ['OGG 320', '320K'],
    ['MP3 320', '320K'],
    ['AAC 192', '192K'],
    ['OGG 192', '192K'],
    ['MP3 128', '128K'],
    ['AAC 96', '96K'],
    ['AAC 48', '48K'],
  ];
  for (const [key, label] of order) {
    const audioUrl = links && links[key];
    if (!audioUrl) continue;
    const inferred = inferQualityFromUrl(audioUrl, key);
    return {
      audioUrl,
      quality: inferred.quality,
      qualityLabel: inferred.qualityLabel || label,
      qualityText: key,
    };
  }
  return { audioUrl: '', quality: '', qualityLabel: '', qualityText: '' };
}

async function detailJoox(params) {
  const decoded = decodeSquareId(params.id || '');
  params = { ...decoded.payload, ...params };
  const keyword = params.keyword || `${params.name || ''} ${params.artist || ''}`.trim();
  const n = params.index || 1;
  const api = `https://apicx.asia/api/joox_music?msg=${encodeURIComponent(keyword)}&n=${encodeURIComponent(n)}&token=${encodeURIComponent(JOOX_TOKEN)}&br=${encodeURIComponent(JOOX_BR)}`;
  const json = await fetchJson(api);
  if (!json || json.code !== 200 || !json.data) throw new Error('Invalid JOOX detail response');
  const data = json.data;
  const best = pickBestJooxUrl(data[JOOX_KEYS.playLinks] || data.playLinks || {});
  return buildTrack('joox', {
    ...params,
    id: data[JOOX_KEYS.songId] || params.id,
    mid: data.songmid || params.mid,
    name: data[JOOX_KEYS.songName] || params.name,
    artist: data[JOOX_KEYS.artist] || params.artist,
    album: data[JOOX_KEYS.album] || params.album,
    audioUrl: best.audioUrl || params.audioUrl,
    lrc: data[JOOX_KEYS.lyric] || params.lrc,
    quality: best.quality,
    qualityLabel: best.qualityLabel,
    needsDetail: false,
  });
}

const detailers = { netease: detailNetease, qq: detailQQ, kuwo: detailKuwo, joox: detailJoox };

function playbackPayload(track, extra = {}) {
  if (track && track.audioUrl) {
    return {
      provider: track.source,
      source: track.source,
      url: track.audioUrl,
      playable: true,
      trial: false,
      level: track.quality || track.qualityLabel || 'standard',
      quality: track.qualityLabel || track.quality || '',
      br: 0,
      loggedIn: false,
      ...extra,
    };
  }
  return {
    provider: track && track.source || extra.source || '',
    source: track && track.source || extra.source || '',
    url: '',
    playable: false,
    trial: false,
    reason: 'url_unavailable',
    message: '当前音源没有返回可播放地址',
    restriction: {
      category: 'url_unavailable',
      message: '当前音源没有返回可播放地址',
      action: 'switch_source',
    },
    loggedIn: false,
    ...extra,
  };
}

async function detailFromEncodedId(rawId) {
  const decoded = decodeSquareId(rawId || '');
  const source = decoded.source || 'netease';
  const cached = cachedTrack(source, rawId) || cachedTrack(source, decoded.payload.id) || {};
  if (!detailers[source]) throw new Error('Unsupported source');
  const track = await detailers[source]({
    ...cached,
    ...decoded.payload,
    source,
    id: rawId,
    nativeId: decoded.payload.id,
  });
  return rememberTrack(track);
}

async function lyricPayloadForTrack(track) {
  const hydrated = track && (track.lrc || track.source === 'qq') ? track : await detailFromEncodedId(track && track.id);
  return {
    provider: hydrated.source,
    source: hydrated.source,
    lyric: hydrated.lrc || '',
    tlyric: '',
    yrc: '',
  };
}

function paramsToTrack(url) {
  return {
    source: url.searchParams.get('source') || '',
    id: url.searchParams.get('id') || '',
    mid: url.searchParams.get('mid') || '',
    mediaMid: url.searchParams.get('mediaMid') || '',
    index: clampInt(url.searchParams.get('index'), 1, 200, 1),
    keyword: url.searchParams.get('keyword') || '',
    name: url.searchParams.get('name') || '',
    artist: url.searchParams.get('artist') || '',
    album: url.searchParams.get('album') || '',
    cover: url.searchParams.get('cover') || '',
    audioUrl: url.searchParams.get('audioUrl') || '',
    lrcUrl: url.searchParams.get('lrcUrl') || '',
    lrc: url.searchParams.get('lrc') || '',
    pay: url.searchParams.get('pay') || '',
  };
}

function headersForMediaRequest(mediaUrl, range, source) {
  const headers = { 'User-Agent': UA, Accept: '*/*' };
  const referers = {
    netease: 'https://music.163.com/',
    qq: 'https://y.qq.com/',
    kuwo: 'https://www.kuwo.cn/',
    joox: 'https://www.joox.com/',
  };
  try {
    const host = new URL(mediaUrl).hostname.toLowerCase();
    if (host.includes('qq.com') || host.includes('qpic.cn')) headers.Referer = referers.qq;
    else if (host.includes('kuwo')) headers.Referer = referers.kuwo;
    else if (host.includes('joox')) headers.Referer = referers.joox;
    else headers.Referer = referers[source] || referers.netease;
  } catch (e) {
    headers.Referer = referers[source] || referers.netease;
  }
  if (range) headers.Range = range;
  return headers;
}

function isAllowedRemoteUrl(raw) {
  try {
    const parsed = new URL(raw);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch (e) {
    return false;
  }
}

async function proxyStream(req, res, targetUrl, source) {
  if (!isAllowedRemoteUrl(targetUrl)) {
    sendError(res, 400, 'Invalid media url');
    return;
  }
  const controller = new AbortController();
  req.on('close', () => controller.abort());
  try {
    const upstream = await fetch(targetUrl, {
      headers: headersForMediaRequest(targetUrl, req.headers.range, source),
      redirect: 'follow',
      signal: controller.signal,
    });
    if (!upstream.ok && upstream.status !== 206) {
      sendError(res, upstream.status || 502, `Upstream media failed: ${upstream.status}`);
      return;
    }
    const headers = {
      'Content-Type': upstream.headers.get('content-type') || 'audio/mpeg',
      'Accept-Ranges': upstream.headers.get('accept-ranges') || 'bytes',
      'Cache-Control': 'public, max-age=3600',
      'Access-Control-Allow-Origin': '*',
    };
    ['content-length', 'content-range'].forEach(name => {
      const value = upstream.headers.get(name);
      if (value) headers[name.replace(/\b\w/g, char => char.toUpperCase())] = value;
    });
    res.writeHead(upstream.status, headers);
    if (!upstream.body) {
      res.end();
      return;
    }
    const stream = Readable.fromWeb(upstream.body);
    stream.on('error', err => {
      if (err && err.name === 'AbortError') return;
      if (!res.destroyed) res.destroy(err);
    });
    stream.pipe(res);
  } catch (err) {
    if (!res.headersSent) sendError(res, 502, err.message || 'Media proxy failed');
    else res.destroy(err);
  }
}

function safeStaticPath(pathname) {
  const decoded = decodeURIComponent(pathname);
  const requested = decoded === '/' ? '/index.html' : decoded;
  const joined = path.join(PUBLIC_DIR, requested);
  const normalized = path.normalize(joined);
  if (!normalized.startsWith(PUBLIC_DIR)) return null;
  return normalized;
}

function serveStatic(req, res, pathname) {
  const filePath = safeStaticPath(pathname);
  if (!filePath) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }
  fs.stat(filePath, (err, stat) => {
    const finalPath = !err && stat.isDirectory() ? path.join(filePath, 'index.html') : filePath;
    fs.readFile(finalPath, (readErr, data) => {
      if (readErr) {
        fs.readFile(path.join(PUBLIC_DIR, 'index.html'), (fallbackErr, fallback) => {
          if (fallbackErr) {
            res.writeHead(404);
            res.end('Not Found');
            return;
          }
          res.writeHead(200, { 'Content-Type': MIME['.html'] });
          res.end(fallback);
        });
        return;
      }
      const ext = path.extname(finalPath).toLowerCase();
      res.writeHead(200, {
        'Content-Type': MIME[ext] || 'application/octet-stream',
        'Cache-Control': ext === '.html' ? 'no-store' : 'public, max-age=3600',
      });
      res.end(data);
    });
  });
}

async function routeApi(req, res, url) {
  const pathname = url.pathname;

  if (pathname === '/api/health') {
    sendJson(res, { ok: true, name: 'Mineradio Square', sources: Object.values(SOURCE_META) });
    return;
  }

  if (pathname === '/api/sources') {
    sendJson(res, { sources: Object.values(SOURCE_META) });
    return;
  }

  if (pathname === '/api/app/version') {
    sendJson(res, {
      name: 'mineradio-square',
      productName: 'Mineradio Square',
      version: '0.1.0',
      update: { configured: false, preview: false },
    });
    return;
  }

  if (pathname === '/api/update/latest') {
    sendJson(res, {
      ok: true,
      hasUpdate: false,
      currentVersion: '0.1.0',
      latestVersion: '0.1.0',
      name: 'Mineradio Square',
      notes: ['Web build uses MusicSquare sources.'],
    });
    return;
  }

  if (pathname === '/api/search') {
    const keyword = normalizeWhitespace(url.searchParams.get('keywords') || url.searchParams.get('q'));
    if (!keyword) {
      sendJson(res, { ok: true, keyword: '', songs: [], sources: [], errors: [] });
      return;
    }
    const selectedSources = url.searchParams.has('sources')
      ? parseSources(url.searchParams.get('sources'))
      : ['netease', 'kuwo', 'joox'];
    const perSourceLimit = clampInt(url.searchParams.get('limit'), 1, 24, 8);
    const totalLimit = clampInt(url.searchParams.get('total'), perSourceLimit, 80, selectedSources.length * perSourceLimit);
    const result = await searchAll(keyword, selectedSources, perSourceLimit, totalLimit);
    sendJson(res, { ok: true, keyword, ...result });
    return;
  }

  if (pathname === '/api/qq/search') {
    const keyword = normalizeWhitespace(url.searchParams.get('keywords') || url.searchParams.get('q'));
    const limit = clampInt(url.searchParams.get('limit'), 1, 24, 12);
    const songs = rememberTracks(keyword ? await searchQQ(keyword, limit) : []);
    sendJson(res, { ok: true, provider: 'qq', songs });
    return;
  }

  if (pathname === '/api/song/url') {
    const id = url.searchParams.get('id') || '';
    const track = await detailFromEncodedId(id);
    sendJson(res, playbackPayload(track));
    return;
  }

  if (pathname === '/api/qq/song/url') {
    const mid = url.searchParams.get('mid') || url.searchParams.get('id') || '';
    const cached = cachedTrack('qq', mid) || {};
    const track = rememberTrack(await detailQQ({ ...cached, id: mid, mid }));
    sendJson(res, playbackPayload(track));
    return;
  }

  if (pathname === '/api/lyric') {
    const id = url.searchParams.get('id') || '';
    const track = await detailFromEncodedId(id);
    sendJson(res, await lyricPayloadForTrack(track));
    return;
  }

  if (pathname === '/api/qq/lyric') {
    const mid = url.searchParams.get('mid') || url.searchParams.get('id') || '';
    const cached = cachedTrack('qq', mid) || {};
    const track = rememberTrack(await detailQQ({ ...cached, id: mid, mid }));
    sendJson(res, await lyricPayloadForTrack(track));
    return;
  }

  if (pathname === '/api/audio' || pathname === '/api/cover' || pathname === '/api/stream') {
    await proxyStream(req, res, url.searchParams.get('url') || '', url.searchParams.get('source') || '');
    return;
  }

  if (pathname === '/api/login/status') {
    sendJson(res, await refreshNeteaseProfile());
    return;
  }

  if (pathname === '/api/qq/login/status') {
    sendJson(res, { provider: 'qq', loggedIn: false, hasCookie: false, playbackKeyReady: false });
    return;
  }

  if (pathname === '/api/logout') {
    neteaseLoginSession.cookie = '';
    neteaseLoginSession.profile = null;
    neteaseLoginSession.account = null;
    neteaseLoginSession.updatedAt = Date.now();
    sendJson(res, { ok: true, loggedIn: false });
    return;
  }

  if (pathname === '/api/qq/logout') {
    sendJson(res, { ok: true, loggedIn: false });
    return;
  }

  if (pathname === '/api/login/qr/key') {
    sendJson(res, await createNeteaseQrKey());
    return;
  }

  if (pathname === '/api/login/qr/create') {
    const key = url.searchParams.get('key') || '';
    if (!key) {
      sendError(res, 400, 'Missing QR key');
      return;
    }
    sendJson(res, createNeteaseQrImage(key));
    return;
  }

  if (pathname === '/api/login/qr/check') {
    const key = url.searchParams.get('key') || '';
    if (!key) {
      sendError(res, 400, 'Missing QR key');
      return;
    }
    sendJson(res, await checkNeteaseQrLogin(key));
    return;
  }

  if (pathname === '/api/login/cookie' && req.method === 'POST') {
    const body = await readJsonBody(req);
    const cookie = body.cookie || body.Cookie || '';
    if (!cookie) {
      sendError(res, 400, 'Missing cookie');
      return;
    }
    sendJson(res, await loginNeteaseWithCookie(cookie));
    return;
  }

  if (pathname === '/api/discover/home') {
    const login = neteaseLoginPayload();
    sendJson(res, {
      loggedIn: login.loggedIn,
      user: login.loggedIn ? {
        userId: login.userId,
        nickname: login.nickname,
        avatar: login.avatar,
      } : null,
      dailySongs: [],
      playlists: [],
      podcasts: [],
      mode: 'music-square',
      updatedAt: Date.now(),
    });
    return;
  }

  if (pathname === '/api/user/playlists' || pathname === '/api/qq/user/playlists') {
    const provider = pathname.includes('/qq/') ? 'qq' : 'netease';
    const login = provider === 'netease' ? neteaseLoginPayload() : { loggedIn: false };
    sendJson(res, { loggedIn: !!login.loggedIn, provider, playlists: [] });
    return;
  }

  if (pathname === '/api/playlist/tracks' || pathname === '/api/qq/playlist/tracks') {
    sendJson(res, { loggedIn: false, playlist: null, tracks: [] });
    return;
  }

  if (pathname === '/api/song/like/check') {
    sendJson(res, { loggedIn: false, liked: false, ids: [] });
    return;
  }

  if (pathname === '/api/song/like' || pathname === '/api/playlist/create' || pathname === '/api/playlist/add-song') {
    sendJson(res, { ok: false, loggedIn: false, error: 'LOGIN_DISABLED' }, 401);
    return;
  }

  if (pathname === '/api/song/comments' || pathname === '/api/qq/song/comments') {
    sendJson(res, { comments: [], hotComments: [], total: 0, more: false });
    return;
  }

  if (pathname === '/api/artist/detail' || pathname === '/api/qq/artist/detail') {
    sendJson(res, { artist: null, songs: [] });
    return;
  }

  if (pathname.startsWith('/api/podcast/')) {
    sendJson(res, { podcasts: [], programs: [], collections: [], items: [], loggedIn: false, more: false });
    return;
  }

  if (pathname === '/api/weather/ip-location') {
    sendJson(res, { ok: false, location: null });
    return;
  }

  if (pathname === '/api/weather/radio') {
    sendJson(res, {
      ok: true,
      weather: null,
      radio: { title: 'MusicSquare Radio', subtitle: '', seedQueries: [], songs: [], updatedAt: Date.now() },
    });
    return;
  }

  if (pathname === '/api/beatmap/cache/status') {
    sendJson(res, { enabled: false, mode: 'memory-only', reason: 'WEB_SOURCE_BUILD' });
    return;
  }

  if (pathname === '/api/beatmap/cache') {
    sendJson(res, { ok: true, hit: false, enabled: false, mode: 'memory-only' });
    return;
  }

  if (pathname === '/api/track') {
    const params = paramsToTrack(url);
    const source = params.source;
    if (!detailers[source]) {
      sendError(res, 400, 'Unsupported source');
      return;
    }
    const track = await detailers[source](params);
    sendJson(res, { ok: true, track });
    return;
  }

  if (pathname === '/api/stream') {
    await proxyStream(req, res, url.searchParams.get('url') || '', url.searchParams.get('source') || '');
    return;
  }

  sendError(res, 404, 'Unknown API endpoint');
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,Range',
    });
    res.end();
    return;
  }

  if (url.pathname.startsWith('/api/')) {
    try {
      await routeApi(req, res, url);
    } catch (err) {
      console.error('[API]', url.pathname, err);
      sendError(res, 500, err.message || 'Internal server error');
    }
    return;
  }

  serveStatic(req, res, url.pathname);
});

server.listen(PORT, HOST, () => {
  console.log(`Mineradio Square listening on http://localhost:${PORT}`);
});
