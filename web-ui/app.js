// Relative, same-origin paths -- serve.py reverse-proxies these to the
// backend services so the browser never makes a cross-origin request (see
// serve.py for why: cross-port requests are unreliable in remote/sandboxed
// dev environments that only forward the one port you navigated to).
const INGEST_URL = '/api/ingest';
const FEED_URL = '/api/feed';

const SWATCHES = [
  { color: '#F5B14C', emoji: '🌅' },
  { color: '#4C9AF5', emoji: '🌊' },
  { color: '#7ED957', emoji: '🌿' },
  { color: '#F5588A', emoji: '🎨' },
  { color: '#8A3AB9', emoji: '✨' },
  { color: '#2D2D2D', emoji: '🌙' },
];

const state = {
  users: [],
  usersById: new Map(),
  currentUserId: null,
  following: new Set(),
  selectedSwatch: SWATCHES[0],
  nextCursor: null,
};

const el = (id) => document.getElementById(id);

function setStatus(msg) {
  el('statusLine').textContent = msg;
  if (msg) setTimeout(() => { if (el('statusLine').textContent === msg) el('statusLine').textContent = ''; }, 4000);
}

// A self-contained SVG data: URI placeholder image -- no network dependency,
// no MinIO upload flow wired into this sample UI (the real presign endpoint
// exists at POST /v1/uploads/presign for a production client to use).
function placeholderImage(color, emoji) {
  const svg = `<svg xmlns='http://www.w3.org/2000/svg' width='400' height='300'>` +
    `<rect width='100%' height='100%' fill='${color}'/>` +
    `<text x='50%' y='53%' font-size='90' text-anchor='middle' dominant-baseline='middle'>${emoji}</text>` +
    `</svg>`;
  return 'data:image/svg+xml,' + encodeURIComponent(svg);
}

function avatarColor(username) {
  let hash = 0;
  for (let i = 0; i < username.length; i++) hash = username.charCodeAt(i) + ((hash << 5) - hash);
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 60%, 50%)`;
}

function initials(username) {
  const cleaned = username.replace(/^user_/, '').replace(/^celeb_/, '');
  return cleaned.slice(0, 2).toUpperCase();
}

function avatarEl(username) {
  const div = document.createElement('div');
  div.className = 'avatar';
  div.style.background = avatarColor(username);
  div.textContent = initials(username);
  return div;
}

function timeAgo(iso) {
  if (!iso) return '';
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (seconds < 60) return 'just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

async function api(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.status === 204 ? null : res.json();
}

async function loadUsers() {
  state.users = await api(`${INGEST_URL}/v1/users`);
  state.usersById = new Map(state.users.map((u) => [u.userId, u]));

  const select = el('userSelect');
  select.innerHTML = '';
  for (const u of state.users) {
    const opt = document.createElement('option');
    opt.value = u.userId;
    opt.textContent = `${u.username}${u.isCelebrity ? ' 🌟' : ''}`;
    select.appendChild(opt);
  }
  if (!state.currentUserId) state.currentUserId = state.users[0]?.userId ?? null;
  select.value = state.currentUserId;
}

async function loadFollowing() {
  if (!state.currentUserId) return;
  const list = await api(`${INGEST_URL}/v1/users/${state.currentUserId}/following`);
  state.following = new Set(list);
}

function renderPeopleList() {
  const ul = el('peopleList');
  ul.innerHTML = '';
  for (const u of state.users) {
    if (u.userId === state.currentUserId) continue;
    const li = document.createElement('li');
    li.className = 'person-row';

    const meta = document.createElement('div');
    meta.className = 'person-meta';
    meta.innerHTML = `<div class="person-username">${u.username}${u.isCelebrity ? ' 🌟' : ''}</div>` +
      `<div class="person-sub">${u.followerCount} followers</div>`;

    const btn = document.createElement('button');
    const isFollowing = state.following.has(u.userId);
    btn.className = 'follow-btn' + (isFollowing ? ' following' : '');
    btn.textContent = isFollowing ? 'Following' : 'Follow';
    btn.onclick = () => toggleFollow(u.userId, !isFollowing);

    li.appendChild(avatarEl(u.username));
    li.appendChild(meta);
    li.appendChild(btn);
    ul.appendChild(li);
  }
}

async function toggleFollow(targetId, follow) {
  try {
    await api(`${INGEST_URL}/v1/${follow ? 'follow' : 'unfollow'}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ followerId: state.currentUserId, followeeId: targetId }),
    });
    if (follow) state.following.add(targetId); else state.following.delete(targetId);
    renderPeopleList();
    await loadUsers(); // follower counts changed
    renderPeopleList();
    setStatus(follow ? 'Followed' : 'Unfollowed');
  } catch (err) {
    setStatus('Error: ' + err.message);
  }
}

function renderSwatches() {
  const container = el('swatchPicker');
  container.innerHTML = '';
  for (const swatch of SWATCHES) {
    const div = document.createElement('div');
    div.className = 'swatch' + (swatch === state.selectedSwatch ? ' selected' : '');
    div.style.background = swatch.color;
    div.title = swatch.emoji;
    div.onclick = () => { state.selectedSwatch = swatch; renderSwatches(); };
    container.appendChild(div);
  }
}

function feedItemCard(item) {
  const li = document.createElement('li');

  if (item.isAd) {
    li.className = 'post-card ad-card';
    li.innerHTML = `
      <div class="post-header">
        <div class="avatar" style="background:#bbb">Ad</div>
        <div class="username">Sponsored</div>
        <div class="source-tag"><span class="badge ad"></span>ad</div>
      </div>
      <div class="post-body"><p class="post-caption">${item.caption ?? 'Sponsored content'}</p></div>`;
    return li;
  }

  const author = state.usersById.get(item.authorId);
  const username = author ? author.username : `user_${item.authorId}`;
  li.className = 'post-card';

  const header = document.createElement('div');
  header.className = 'post-header';
  header.appendChild(avatarEl(username));
  const uname = document.createElement('div');
  uname.className = 'username';
  uname.textContent = username;
  header.appendChild(uname);
  const sourceTag = document.createElement('div');
  sourceTag.className = 'source-tag';
  sourceTag.innerHTML = `<span class="badge ${item.source}"></span>${item.source}`;
  header.appendChild(sourceTag);
  const ts = document.createElement('div');
  ts.className = 'timestamp';
  ts.textContent = timeAgo(item.createdAt);
  header.appendChild(ts);
  li.appendChild(header);

  if (item.mediaUrl) {
    const mediaDiv = document.createElement('div');
    mediaDiv.className = 'post-media';
    const img = document.createElement('img');
    img.src = item.mediaUrl;
    img.alt = item.caption ?? '';
    mediaDiv.appendChild(img);
    li.appendChild(mediaDiv);
  }

  const body = document.createElement('div');
  body.className = 'post-body';
  body.innerHTML = `<p class="post-caption">${escapeHtml(item.caption ?? '')}</p>`;

  const actions = document.createElement('div');
  actions.className = 'post-actions';

  const likeBtn = document.createElement('button');
  likeBtn.className = 'action-btn like-btn' + (item.likedByMe ? ' liked' : '');
  likeBtn.innerHTML = `<span class="like-icon">${item.likedByMe ? '❤️' : '🤍'}</span> <span class="like-count">${item.likeCount}</span>`;
  likeBtn.onclick = () => toggleLike(item.postId, !item.likedByMe, likeBtn);
  actions.appendChild(likeBtn);

  const commentToggle = document.createElement('button');
  commentToggle.className = 'action-btn comment-toggle';
  commentToggle.innerHTML = `💬 <span class="comment-count">${item.commentCount}</span>`;
  const scoreTag = document.createElement('span');
  scoreTag.className = 'score-tag';
  scoreTag.textContent = `score ${item.score.toFixed(2)}`;
  actions.appendChild(commentToggle);
  actions.appendChild(scoreTag);
  body.appendChild(actions);

  const commentSection = document.createElement('div');
  commentSection.className = 'comment-section';
  commentSection.hidden = true;
  const bumpCommentCount = () => {
    const countEl = commentToggle.querySelector('.comment-count');
    if (countEl) countEl.textContent = String(Number(countEl.textContent) + 1);
  };
  commentToggle.onclick = () => {
    commentSection.hidden = !commentSection.hidden;
    if (!commentSection.hidden) loadComments(item.postId, commentSection, bumpCommentCount);
  };
  body.appendChild(commentSection);

  li.appendChild(body);
  return li;
}

async function toggleLike(postId, like, btn) {
  btn.disabled = true;
  try {
    const resp = await api(`${INGEST_URL}/v1/posts/${postId}/${like ? 'like' : 'unlike'}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: state.currentUserId }),
    });
    btn.classList.toggle('liked', resp.liked);
    btn.querySelector('.like-icon').textContent = resp.liked ? '❤️' : '🤍';
    btn.querySelector('.like-count').textContent = resp.likeCount;
    btn.onclick = () => toggleLike(postId, !resp.liked, btn);
  } catch (err) {
    setStatus('Like failed: ' + err.message);
  } finally {
    btn.disabled = false;
  }
}

async function loadComments(postId, container, onCommentAdded) {
  container.innerHTML = '<p class="hint">Loading comments...</p>';
  try {
    const comments = await api(`${INGEST_URL}/v1/posts/${postId}/comments`);
    container.innerHTML = '';
    const list = document.createElement('ul');
    list.className = 'comment-list';
    if (comments.length === 0) {
      const empty = document.createElement('li');
      empty.className = 'hint';
      empty.textContent = 'No comments yet.';
      list.appendChild(empty);
    }
    for (const c of comments) {
      const li = document.createElement('li');
      li.innerHTML = `<strong>${escapeHtml(c.username)}</strong> ${escapeHtml(c.body)}`;
      list.appendChild(li);
    }
    container.appendChild(list);

    const form = document.createElement('div');
    form.className = 'comment-form';
    const input = document.createElement('input');
    input.type = 'text';
    input.placeholder = 'Add a comment...';
    const sendBtn = document.createElement('button');
    sendBtn.textContent = 'Comment';
    sendBtn.onclick = async () => {
      const body = input.value.trim();
      if (!body) return;
      sendBtn.disabled = true;
      try {
        await api(`${INGEST_URL}/v1/posts/${postId}/comments`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ userId: state.currentUserId, body }),
        });
        input.value = '';
        onCommentAdded?.();
        await loadComments(postId, container, onCommentAdded);
      } catch (err) {
        setStatus('Comment failed: ' + err.message);
      } finally {
        sendBtn.disabled = false;
      }
    };
    form.appendChild(input);
    form.appendChild(sendBtn);
    container.appendChild(form);
  } catch (err) {
    container.innerHTML = `<p class="hint">Failed to load comments: ${escapeHtml(err.message)}</p>`;
  }
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

async function loadFeed(reset) {
  if (!state.currentUserId) return;
  if (reset) {
    el('feedList').innerHTML = '';
    state.nextCursor = null;
  }
  // NOTE: `new URL(FEED_URL + ...)` doesn't work here -- FEED_URL is a
  // relative same-origin path (see the comment at the top of this file),
  // and the URL constructor throws "Invalid URL" on a relative string
  // without an explicit base. Build the query string by hand instead.
  const params = new URLSearchParams({ userId: state.currentUserId });
  if (state.nextCursor) params.set('cursor', state.nextCursor);

  try {
    const resp = await api(`${FEED_URL}/v1/feed?${params.toString()}`);
    const ul = el('feedList');
    for (const item of resp.items) ul.appendChild(feedItemCard(item));
    state.nextCursor = resp.nextCursor || null;
    el('loadMoreBtn').hidden = !state.nextCursor || resp.items.length === 0;
    el('emptyState').hidden = ul.children.length > 0;
  } catch (err) {
    setStatus('Feed error: ' + err.message);
  }
}

async function submitPost() {
  const caption = el('captionInput').value.trim();
  if (!caption) { setStatus('Write something first'); return; }

  const btn = el('postBtn');
  btn.disabled = true;
  try {
    const mediaUrl = placeholderImage(state.selectedSwatch.color, state.selectedSwatch.emoji);
    await api(`${INGEST_URL}/v1/posts`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: state.currentUserId, mediaUrl, mediaType: 1, caption }),
    });
    el('captionInput').value = '';
    setStatus('Posted! Fan-out/vector/notification workers are processing async — refreshing in 3s...');
    setTimeout(() => loadFeed(true), 3000);
  } catch (err) {
    setStatus('Post failed: ' + err.message);
  } finally {
    btn.disabled = false;
  }
}

async function resetSeen() {
  try {
    await api(`${FEED_URL}/v1/feed/reset-seen`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: state.currentUserId }),
    });
    setStatus('Seen-state cleared for this account (demo only) -- reloading feed...');
    await loadFeed(true);
  } catch (err) {
    setStatus('Reset failed: ' + err.message);
  }
}

async function onUserChange() {
  state.currentUserId = el('userSelect').value;
  await loadFollowing();
  renderPeopleList();
  await loadFeed(true);
}

async function init() {
  // Wire up event listeners FIRST, before any await that could throw --
  // otherwise a single failed fetch during startup leaves every button
  // dead with no visible sign why (this bit us once: a leftover `new
  // URL()` call with no base threw here and silently skipped all the
  // addEventListener calls below it).
  el('userSelect').addEventListener('change', onUserChange);
  el('postBtn').addEventListener('click', submitPost);
  el('refreshBtn').addEventListener('click', () => loadFeed(true));
  el('loadMoreBtn').addEventListener('click', () => loadFeed(false));
  el('resetSeenBtn').addEventListener('click', resetSeen);

  renderSwatches();
  await loadUsers();
  await loadFollowing();
  renderPeopleList();
  await loadFeed(true);
}

init().catch((err) => setStatus('Init error: ' + err.message));
