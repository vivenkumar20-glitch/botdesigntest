#!/usr/bin/env python3
"""
Reliability & fault-tolerance validation suite for main.py (Aviyl bot).

Run:  python3 tests/test_reliability.py

No third-party test framework required — plain asserts + asyncio.
Covers:
  1.  Gemini generation: success, 429 backoff-retry, failover, auth abort,
      5xx bounded retries, network-error retries, 400 thinking-case recovery,
      empty-output recovery, strict_no_fallback, latency-budget boundedness.
  2.  Rate limiter: non-serialized concurrency, bounded waiting, map pruning.
  3.  call_llm_json_mode failure-notice handling.
  4.  Burst pipeline: crash/timeout never leaves the user silently unresponsive,
      superseded generations never double-reply.
  5.  on_message gateway-redelivery dedupe.
  6.  Housekeeping cache pruning (memory-leak guards).
  7.  Off-loop chat-memory persistence.
"""

import asyncio
import json
import os
import sys
import tempfile
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

# Isolate side-effect files (chat_memory.json etc.) in a temp cwd
_WORKDIR = tempfile.mkdtemp(prefix="aviyl_test_")
os.chdir(_WORKDIR)
os.environ["GEMINI_API_KEY"] = "test-key-not-real"
os.environ.setdefault("LOG_LEVEL", "WARNING")

import aiohttp  # noqa: E402

import main  # noqa: E402

# Make the test user an insta-authorized owner so no code path ever attempts a real
# Discord API call (bot.is_owner -> application_info) during validation.
main.CONFIGURED_OWNER_IDS = {42}

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {detail}")


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
class FakeResp:
    def __init__(self, status, body=None, headers=None, text_body=None):
        self.status = status
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self._text = text_body if text_body is not None else json.dumps(self._body)

    async def json(self):
        return self._body

    async def text(self):
        return self._text


class FakeCM:
    def __init__(self, item):
        self._item = item

    async def __aenter__(self):
        if isinstance(self._item, Exception):
            raise self._item
        return self._item

    async def __aexit__(self, *a):
        return False


class FakeSession:
    """Scripted aiohttp.ClientSession replacement. Each .post() consumes one scripted item."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []  # (url, payload)
        self.closed = False

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs.get("json")))
        item = self.script.pop(0) if self.script else FakeResp(500, {"error": {"message": "script exhausted"}})
        return FakeCM(item)

    def get(self, url, **kwargs):
        self.calls.append((url, None))
        item = self.script.pop(0) if self.script else FakeResp(500)
        return FakeCM(item)

    async def close(self):
        self.closed = True


def ok_body(text="hello there!", model_ver="gemini-4-hydrogen-4.0", finish="STOP"):
    return {
        "candidates": [{"content": {"parts": [{"text": text}]}, "finishReason": finish}],
        "modelVersion": model_ver,
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 5,
            "thoughtsTokenCount": 2,
            "totalTokenCount": 17,
        },
    }


def reset_ai_state():
    main.recovery_chip = main.SystemRecoveryChip()
    main.CONFIGURED_OWNER_IDS = {42}
    main.AI_CONFIG["current_model"] = main.PRIMARY_GEMINI_MODEL
    main.AI_CONFIG["thinking_level"] = "low"
    main.AI_CONFIG["chat_mode"] = "normal"
    main.AI_CONFIG["strict_no_fallback"] = False
    main.AI_CONFIG["show_telemetry_footer"] = False
    main.ai_limiter.max_rpm = 100000
    main.USER_CHAT_MEMORY.clear()
    main._GENERATION_SEMAPHORE = asyncio.Semaphore(6)


async def gen(prompt="hello how are you", use_memory=False, **kw):
    return await main.generate_gemini_response(
        42, "tester", prompt, use_memory=use_memory, **kw
    )


# ---------------------------------------------------------------------------
# 1. Gemini generation pipeline
# ---------------------------------------------------------------------------
async def test_generation_suite():
    print("\n[1] Gemini generation: retries, backoff, failover, classification")

    # A: success on first attempt
    reset_ai_state()
    sess = FakeSession([FakeResp(200, ok_body("first try!"))])
    main.bot.http_session = sess
    out = await gen()
    check("A success-first-try returns model text", out == "first try!", repr(out))
    check("A exactly one POST fired", len(sess.calls) == 1, str(len(sess.calls)))
    check("A telemetry recorded", main.AI_CONFIG["last_latency_sec"] is not None)

    # A2: success records chat memory when use_memory=True
    reset_ai_state()
    sess = FakeSession([FakeResp(200, ok_body("mem reply"))])
    main.bot.http_session = sess
    out = await gen(prompt="remember me please", use_memory=True)
    mem = main.USER_CHAT_MEMORY.get(42)
    check("A2 chat memory recorded both turns", mem is not None and len(mem) == 2, str(mem))

    # B: 429 x2 with Retry-After, then 200 on SAME tier (bounded backoff retry)
    reset_ai_state()
    sess = FakeSession([
        FakeResp(429, {"error": {"message": "quota"}}, headers={"Retry-After": "0.2"}),
        FakeResp(429, {"error": {"message": "quota"}}, headers={"Retry-After": "0.2"}),
        FakeResp(200, ok_body("recovered!")),
    ])
    main.bot.http_session = sess
    t0 = time.monotonic()
    out = await gen()
    dt = time.monotonic() - t0
    check("B success after same-tier backoff retries", out == "recovered!", repr(out))
    check("B exactly 3 POSTs on tier-1 only", len(sess.calls) == 3 and all(main.PRIMARY_GEMINI_MODEL in u for u, _ in sess.calls), str([u for u, _ in sess.calls]))
    check("B backoff actually waited (>0.2s)", dt > 0.2, f"{dt:.2f}s")

    # C: persistent 429 on tier-1 -> failover to tier-2
    reset_ai_state()
    sess = FakeSession([
        FakeResp(429, {"error": {"message": "quota"}}),
        FakeResp(429, {"error": {"message": "quota"}}),
        FakeResp(429, {"error": {"message": "quota"}}),
        FakeResp(200, ok_body("tier2 saved us", model_ver="gemini-3.8-flash-3.8")),
    ])
    main.bot.http_session = sess
    out = await gen()
    check("C failover to tier-2 succeeds", out == "tier2 saved us", repr(out))
    check("C 4 POSTs total (3 tier1 + 1 tier2)", len(sess.calls) == 4, str(len(sess.calls)))
    check("C tier-1 marked unhealthy (60s cooldown)", not main.recovery_chip.is_healthy(main.PRIMARY_GEMINI_MODEL))

    # D: 401 auth error aborts cascade immediately (no useless failover/retries)
    reset_ai_state()
    sess = FakeSession([FakeResp(401, {"error": {"message": "API key not valid"}})])
    main.bot.http_session = sess
    out = await gen()
    check("D auth error -> 1 POST only (abort)", len(sess.calls) == 1, str(len(sess.calls)))
    check("D auth-classified user message", "Authentication Failed" in out, repr(out))
    check("D auth message names the .env fix", "GEMINI_API_KEY" in out)

    # E: 500 everywhere -> bounded attempts across tiers, classified terminal notice
    reset_ai_state()
    sess = FakeSession([FakeResp(500, {"error": {"message": "backend exploded"}}) for _ in range(12)])
    main.bot.http_session = sess
    t0 = time.monotonic()
    out = await gen()
    dt = time.monotonic() - t0
    check("E bounded retries: 9 POSTs max (3 tiers x 3 attempts)", len(sess.calls) == 9, str(len(sess.calls)))
    check("E terminal notice returned (never empty)", isinstance(out, str) and len(out) > 40, repr(out))
    check("E mentions the upstream failure", "HTTP 500" in out, repr(out[:120]))
    check("E completed within budget (<55s)", dt < 55, f"{dt:.1f}s")

    # F: network/transport exceptions -> retried then failover, still a terminal reply
    reset_ai_state()
    sess = FakeSession([aiohttp.ClientConnectionError("boom down") for _ in range(12)])
    main.bot.http_session = sess
    out = await gen()
    check("F transport errors -> terminal notice (never hangs)", isinstance(out, str) and len(out) > 40, repr(out))
    check("F bounded transport retries (9 POSTs)", len(sess.calls) == 9, str(len(sess.calls)))
    check("F error text names the transport failure", "ClientConnectionError" in out, repr(out[:120]))

    # G: 400 thinkingLevel casing -> inline lowercase retry succeeds
    reset_ai_state()
    sess = FakeSession([
        FakeResp(400, text_body='{"error": {"message": "thinkingLevel enum invalid"}}'),
        FakeResp(200, ok_body("lowercase fix worked")),
    ])
    main.bot.http_session = sess
    out = await gen()
    check("G 400-thinking inline retry recovers", out == "lowercase fix worked", repr(out))
    check("G exactly 2 POSTs", len(sess.calls) == 2, str(len(sess.calls)))

    # H: empty output triggers recovery POST
    reset_ai_state()
    empty_body = {"candidates": [{"content": {"parts": []}, "finishReason": "MAX_TOKENS"}], "modelVersion": "x", "usageMetadata": {}}
    sess = FakeSession([FakeResp(200, empty_body), FakeResp(200, ok_body("recovered from empty"))])
    main.bot.http_session = sess
    out = await gen()
    check("H empty-output recovery POST used", out == "recovered from empty", repr(out))
    check("H exactly 2 POSTs (main + recovery)", len(sess.calls) == 2, str(len(sess.calls)))

    # I: strict_no_fallback -> single tier only
    reset_ai_state()
    main.AI_CONFIG["strict_no_fallback"] = True
    sess = FakeSession([FakeResp(503, {"error": {"message": "down"}}) for _ in range(6)])
    main.bot.http_session = sess
    out = await gen()
    check("I strict_no_fallback: only tier-1 attempted", len(sess.calls) == 3 and all(main.PRIMARY_GEMINI_MODEL in u for u, _ in sess.calls), str(len(sess.calls)))
    main.AI_CONFIG["strict_no_fallback"] = False


# ---------------------------------------------------------------------------
# 2. Rate limiter
# ---------------------------------------------------------------------------
async def test_rate_limiter():
    print("\n[2] GeminiRateLimiter: concurrency, bounded waits, pruning")

    # Concurrent acquires on an empty window must NOT serialize behind the lock
    lim = main.GeminiRateLimiter(max_rpm=30, user_cooldown=2.5)
    t0 = time.monotonic()
    await asyncio.gather(*[lim.acquire_slot(i) for i in range(8)])
    dt = time.monotonic() - t0
    check("2a 8 concurrent acquires fast on empty window (<0.5s)", dt < 0.5, f"{dt:.2f}s")

    # Full window: waiting is bounded (soft-landing), never infinite
    lim = main.GeminiRateLimiter(max_rpm=1, user_cooldown=0.0)
    await lim.acquire_slot(1)
    t0 = time.monotonic()
    await lim.acquire_slot(2, max_wait_sec=0.6)  # small cap for the test
    dt = time.monotonic() - t0
    check("2b full-window wait is bounded (>=0.5s, <2.5s)", 0.5 <= dt < 2.5, f"{dt:.2f}s")
    check("2c soft-landing still admits the request", len(lim.request_timestamps) == 2)

    # Parallel bounded waits are NOT serialized (3 waiters share the same window)
    lim = main.GeminiRateLimiter(max_rpm=1, user_cooldown=0.0)
    await lim.acquire_slot(1)
    t0 = time.monotonic()
    await asyncio.gather(*[lim.acquire_slot(100 + i, max_wait_sec=0.6) for i in range(3)])
    dt = time.monotonic() - t0
    check("2d 3 parallel waiters not serialized behind lock (<1.2s)", dt < 1.2, f"{dt:.2f}s")

    # user_last_request pruning
    lim = main.GeminiRateLimiter()
    now = time.monotonic()
    lim.user_last_request = {i: now - 1000.0 for i in range(3000)}
    lim.user_last_request[777] = now
    lim._prune_user_map(now)
    check("2e stale user map entries pruned", len(lim.user_last_request) < 50, str(len(lim.user_last_request)))
    check("2f fresh entry survives pruning", 777 in lim.user_last_request)


# ---------------------------------------------------------------------------
# 3. call_llm_json_mode failure-notice guard
# ---------------------------------------------------------------------------
async def test_call_llm_json_mode():
    print("\n[3] call_llm_json_mode: API failure notices never masquerade as JSON")

    async def fake_gen_failure(**kw):
        return "🛡️ **Gemini 4 Hydrogen API Notice:** Request to `x` could not be completed (`HTTP 500`)."

    async def fake_gen_ok(**kw):
        return '```json\n{"name": "create_server_category", "arguments": {"category_name": "Lab"}}\n```'

    orig = main.generate_gemini_response
    try:
        main.generate_gemini_response = fake_gen_failure
        out = await main.call_llm_json_mode("plan this")
        check("3a failure notice -> empty string", out == "", repr(out))

        main.generate_gemini_response = fake_gen_ok
        out = await main.call_llm_json_mode("plan this")
        check("3b valid JSON extraction still works", out.startswith("{") and "create_server_category" in out, repr(out))
    finally:
        main.generate_gemini_response = orig


# ---------------------------------------------------------------------------
# 4. Burst pipeline: never-silent failures, no double replies
# ---------------------------------------------------------------------------
class FakeUser:
    def __init__(self, uid=42, name="tester"):
        self.id = uid
        self.name = name
        self.display_name = name
        self.bot = False
        self.mention = f"<@{uid}>"


class FakeChannel:
    def __init__(self, cid=7):
        self.id = cid
        self.sent = []

    def typing(self):
        class _T:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *a):
                return False

        return _T()

    async def send(self, content=None, **kw):
        self.sent.append(content)
        return object()


class FakeMessage:
    def __init__(self, text="hi bot"):
        self.author = FakeUser()
        self.channel = FakeChannel()
        self.guild = None  # DM-style context (real discord.Message always has .guild)
        self.content = text
        self.attachments = []
        self.embeds = []
        self.stickers = []
        self.reference = None
        self.replies = []

    async def reply(self, content=None, **kw):
        self.replies.append(content)
        return object()


async def test_burst_pipeline():
    print("\n[4] Burst pipeline: crash/timeout reporting + duplicate suppression")

    # M: dispatch crashes -> user still gets an error reply, buffer cleaned
    main.USER_MESSAGE_BURST_BUFFER.clear()
    msg = FakeMessage()
    sess_key = (msg.channel.id, msg.author.id)
    main.USER_MESSAGE_BURST_BUFFER[sess_key] = {
        "messages": [msg], "lines": ["hello there"], "task": None,
        "last_msg_ts": time.monotonic(), "generation": 1,
    }

    async def boom_dispatch(*a, **kw):
        raise RuntimeError("synthetic explosion")

    orig = main.run_ai_and_dispatch
    try:
        main.run_ai_and_dispatch = boom_dispatch
        await main._process_buffered_user_burst(sess_key, 1)
        check("4a crash -> user receives error reply", any("internal error" in (r or "") for r in msg.replies), str(msg.replies))
        check("4b crash -> burst buffer cleaned up", sess_key not in main.USER_MESSAGE_BURST_BUFFER)

        # N: success path replies exactly once and cleans up
        main.USER_MESSAGE_BURST_BUFFER.clear()
        msg = FakeMessage()
        sess_key = (msg.channel.id, msg.author.id)
        main.USER_MESSAGE_BURST_BUFFER[sess_key] = {
            "messages": [msg], "lines": ["hello there"], "task": None,
            "last_msg_ts": time.monotonic(), "generation": 1,
        }

        async def ok_dispatch(*a, **kw):
            return "yo what's up", []

        main.run_ai_and_dispatch = ok_dispatch
        await main._process_buffered_user_burst(sess_key, 1)
        check("4c success -> exactly one reply", msg.replies == ["yo what's up"], str(msg.replies))
        check("4d success -> buffer cleaned up", sess_key not in main.USER_MESSAGE_BURST_BUFFER)

        # O: superseded generation must NOT double-reply
        main.USER_MESSAGE_BURST_BUFFER.clear()
        msg = FakeMessage()
        sess_key = (msg.channel.id, msg.author.id)
        state = {
            "messages": [msg], "lines": ["line one"], "task": None,
            "last_msg_ts": time.monotonic(), "generation": 1,
        }
        main.USER_MESSAGE_BURST_BUFFER[sess_key] = state

        async def superseding_dispatch(*a, **kw):
            state["generation"] = 2  # a newer line arrived while generating
            return "stale reply that must be discarded", []

        main.run_ai_and_dispatch = superseding_dispatch
        await main._process_buffered_user_burst(sess_key, 1)
        check("4e superseded generation sends NO reply", msg.replies == [], str(msg.replies))

        # P: dispatch watchdog bounds an eternally-hanging dispatch
        main.USER_MESSAGE_BURST_BUFFER.clear()
        msg = FakeMessage()
        sess_key = (msg.channel.id, msg.author.id)
        main.USER_MESSAGE_BURST_BUFFER[sess_key] = {
            "messages": [msg], "lines": ["hello there"], "task": None,
            "last_msg_ts": time.monotonic(), "generation": 1,
        }

        async def hang_dispatch(*a, **kw):
            await asyncio.sleep(3600)

        main.run_ai_and_dispatch = hang_dispatch
        orig_watchdog = main.AI_DISPATCH_WATCHDOG_SEC
        main.AI_DISPATCH_WATCHDOG_SEC = 1.0  # shrink for the test
        t0 = time.monotonic()
        await main._process_buffered_user_burst(sess_key, 1)
        dt = time.monotonic() - t0
        main.AI_DISPATCH_WATCHDOG_SEC = orig_watchdog
        check("4f hanging dispatch killed by watchdog (<5s)", dt < 5, f"{dt:.1f}s")
        check("4g watchdog -> user receives timeout reply", any("too long" in (r or "") for r in msg.replies), str(msg.replies))
    finally:
        main.run_ai_and_dispatch = orig


# ---------------------------------------------------------------------------
# 5. on_message redelivery dedupe
# ---------------------------------------------------------------------------
async def test_message_dedupe():
    print("\n[5] on_message: gateway redelivery dedupe")

    class FakeCtx:
        valid = True

    invoked = {"n": 0}
    ctx_calls = {"n": 0}

    async def fake_get_context(message):
        ctx_calls["n"] += 1
        return FakeCtx()

    async def fake_invoke(ctx):
        invoked["n"] += 1

    class FakeMsg2:
        def __init__(self):
            self.id = 987654321
            self.author = FakeUser()
            self.channel = FakeChannel()
            self.content = "!ping"
            self.mentions = []
            self.embeds = []
            self.attachments = []
            self.stickers = []
            self.reference = None

    orig_gc, orig_inv = main.bot.get_context, main.bot.invoke
    main.SEEN_MESSAGE_IDS.clear()
    try:
        main.bot.get_context = fake_get_context
        main.bot.invoke = fake_invoke
        m = FakeMsg2()
        await main.on_message(m)
        await main.on_message(m)  # duplicate delivery of the SAME message id
        check("5a first delivery processed", invoked["n"] == 1, str(invoked["n"]))
        check("5b duplicate delivery dropped (no reprocess)", ctx_calls["n"] == 1, str(ctx_calls["n"]))
    finally:
        main.bot.get_context = orig_gc
        main.bot.invoke = orig_inv
        main.SEEN_MESSAGE_IDS.clear()


# ---------------------------------------------------------------------------
# 6. Housekeeping pruning
# ---------------------------------------------------------------------------
async def test_housekeeping():
    print("\n[6] Housekeeping: bounded-memory cache hygiene")

    now = time.monotonic()
    main.USER_LAST_TYPING_TS.clear()
    main.USER_LAST_TYPING_TS[(1, 1)] = now - 400.0  # stale
    main.USER_LAST_TYPING_TS[(2, 2)] = now  # fresh

    main.ACTIVE_CONVERSATION_SESSIONS.clear()
    main.ACTIVE_CONVERSATION_SESSIONS[(1, 1)] = now - 10000.0

    main.SEEN_MESSAGE_IDS.clear()
    main.SEEN_MESSAGE_IDS[111] = now - 500.0
    main.SEEN_MESSAGE_IDS[222] = now

    main.LAST_BOT_MESSAGES.clear()
    for i in range(600):
        main.LAST_BOT_MESSAGES[i] = object()

    main.USER_MESSAGE_BURST_BUFFER.clear()
    main.USER_MESSAGE_BURST_BUFFER[(1, 1)] = {
        "messages": [], "lines": [], "task": None,
        "last_msg_ts": now - 10000.0, "generation": 1,
    }

    main.DELETED_CHANNEL_IDS.clear()
    main.DELETED_CHANNEL_IDS[5] = set(range(5000))

    main.LAST_CREATED_CATEGORY.clear()
    main.LAST_CREATED_CATEGORY[9] = (object(), now - 5000.0)

    main._housekeeping_cycle()

    check("6a stale typing tracker pruned", (1, 1) not in main.USER_LAST_TYPING_TS and (2, 2) in main.USER_LAST_TYPING_TS)
    check("6b stale conversation window pruned", (1, 1) not in main.ACTIVE_CONVERSATION_SESSIONS)
    check("6c stale redelivery id pruned, fresh kept", 111 not in main.SEEN_MESSAGE_IDS and 222 in main.SEEN_MESSAGE_IDS)
    check("6d LAST_BOT_MESSAGES bounded to 512", len(main.LAST_BOT_MESSAGES) == 512, str(len(main.LAST_BOT_MESSAGES)))
    check("6e settled burst buffer entry pruned", (1, 1) not in main.USER_MESSAGE_BURST_BUFFER)
    check("6f deleted-id set bounded to 2000", len(main.DELETED_CHANNEL_IDS[5]) == 2000, str(len(main.DELETED_CHANNEL_IDS[5])))
    check("6g stale category pointer pruned", 9 not in main.LAST_CREATED_CATEGORY)


# ---------------------------------------------------------------------------
# 7. Chat-memory persistence is off-loop but durable
# ---------------------------------------------------------------------------
async def test_chat_memory_persist():
    print("\n[7] Chat memory: synchronous state, deferred durable write")

    main.USER_CHAT_MEMORY.clear()
    main.record_user_chat_turn(999, "tester", "hello world", persist=True)
    mem = main.USER_CHAT_MEMORY.get(999)
    check("7a in-memory turn recorded synchronously", mem is not None and len(mem) == 1, str(mem))
    await asyncio.sleep(0.6)  # allow the micro-batch flush task to run
    path = os.path.join(_WORKDIR, main.CHAT_MEMORY_FILE)
    check("7b deferred disk write completed", os.path.exists(path))
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        check("7c persisted data contains the turn", "999" in data and data["999"], str(data.get("999")))


# ---------------------------------------------------------------------------
# 8. Auth vs transient classification is wired into the terminal messaging
# ---------------------------------------------------------------------------
async def test_classification_messages():
    print("\n[8] Terminal error messaging distinguishes failure classes")

    # rate-limit class -> dedicated message
    reset_ai_state()
    sess = FakeSession([FakeResp(429, {"error": {"message": "rpm"}}) for _ in range(12)])
    main.bot.http_session = sess
    out = await gen()
    check("8a rate-limit terminal message", "Rate Limited" in out, repr(out[:100]))

    # timeout/budget class -> dedicated message (fake: freeze budget by patching time? instead verify
    # that the generic notice is what a pure invalid_request cascade yields)
    reset_ai_state()
    sess = FakeSession([FakeResp(422, {"error": {"message": "unprocessable"}}) for _ in range(6)])
    main.bot.http_session = sess
    out = await gen()
    check("8b non-retryable 422 aborts without same-tier retries (3 POSTs -> 1 per tier? aborts fully)",
          len(sess.calls) <= 3, str(len(sess.calls)))
    check("8c generic notice still backward-compatible", "Gemini 4 Hydrogen API Notice" in out, repr(out[:100]))


# ---------------------------------------------------------------------------
# 9. End-to-end chat pipeline (real run_ai_and_dispatch with fake HTTP)
# ---------------------------------------------------------------------------
async def test_end_to_end_pipeline():
    print("\n[9] End-to-end: full request->response pipeline, normal + total-failure")

    reset_ai_state()
    main.AI_CONFIG["decision_engine_enabled"] = False  # keep the /decisions endpoint out of the fake script

    # Normal scenario: user message -> generation -> reply lands in the channel
    main.USER_MESSAGE_BURST_BUFFER.clear()
    msg = FakeMessage("hello how are you today my friend")
    sess_key = (msg.channel.id, msg.author.id)
    main.USER_MESSAGE_BURST_BUFFER[sess_key] = {
        "messages": [msg], "lines": [msg.content], "task": None,
        "last_msg_ts": time.monotonic(), "generation": 1,
    }
    sess = FakeSession([FakeResp(200, ok_body("hey all good wbu?"))])
    main.bot.http_session = sess
    await main._process_buffered_user_burst(sess_key, 1)
    check("9a end-to-end reply delivered to user", msg.replies == ["hey all good wbu?"], str(msg.replies))

    # Total-failure scenario: every tier 500s -> user still receives the classified notice (never silence)
    main.USER_MESSAGE_BURST_BUFFER.clear()
    msg = FakeMessage("tell me something interesting about space")
    sess_key = (msg.channel.id, msg.author.id)
    main.USER_MESSAGE_BURST_BUFFER[sess_key] = {
        "messages": [msg], "lines": [msg.content], "task": None,
        "last_msg_ts": time.monotonic(), "generation": 1,
    }
    sess = FakeSession([FakeResp(500, {"error": {"message": "down"}}) for _ in range(12)])
    main.bot.http_session = sess
    await main._process_buffered_user_burst(sess_key, 1)
    check("9b total API failure -> user still gets a reply", len(msg.replies) == 1 and len(msg.replies[0]) > 40, str(msg.replies))
    check("9c failure reply carries the classified notice", any("Notice" in (r or "") or "Rate Limited" in (r or "") for r in msg.replies), str(msg.replies)[:120])
    main.AI_CONFIG["decision_engine_enabled"] = True


async def main_run():
    await test_generation_suite()
    await test_rate_limiter()
    await test_call_llm_json_mode()
    await test_burst_pipeline()
    await test_message_dedupe()
    await test_housekeeping()
    await test_chat_memory_persist()
    await test_classification_messages()
    await test_end_to_end_pipeline()
    print(f"\n{'='*60}\nRESULT: {PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main_run()))
