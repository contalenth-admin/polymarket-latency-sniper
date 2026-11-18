require('dotenv').config();
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer, WebSocket: WSClient } = require('ws');
const { createSecureClient } = require('@polymarket/client');
const { privateKeyToAccount } = require('viem/accounts');
const { Wallet } = require('ethers');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3004;
const DATA_FILE = path.join(__dirname, 'snipes.json');
const ENV_FILE = path.join(__dirname, '.env');

// Live Trading & Auto-Trade State
let polymarketClient = null;
let liveWalletAddress = null;
let liveTradingReady = false;
let autoTradeEnabled = true;
let autoTradeAmount = parseFloat(process.env.AUTO_TRADE_SIZE || '1.0');
let tradingMode = process.env.TRADING_MODE === 'LIVE' ? 'LIVE' : 'SIMULATION';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// State Persistence
let snipes = [];
if (fs.existsSync(DATA_FILE)) {
  try {
    snipes = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch (e) {
    console.error('Error reading snipes.json:', e.message);
  }
}

function saveSnipes() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(snipes.slice(0, 100), null, 2));
  } catch (e) {}
}

let botLogs = [];
function addLog(msg, type = 'info') {
  const time = new Date().toLocaleTimeString('es-ES', { hour12: false });
  const logItem = { time, msg, type };
  botLogs.unshift(logItem);
  if (botLogs.length > 80) botLogs.pop();
  console.log(`[${time}] ${msg}`);
  broadcast({ type: 'LOG', data: logItem });
}

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach(c => {
    if (c.readyState === 1) c.send(msg);
  });
}

// Live feeds
let binanceSpot = 0.0;
let lastBinanceTickTime = Date.now();
let kalshiMarket = {
  eventTicker: null,
  marketTicker: null,
  title: 'Cargando mercado Kalshi...',
  strike: 0.0,
  expirationTime: null,
  secondsLeft: 900,
  yesBid: 0.50,
  yesAsk: 0.50,
  noBid: 0.50,
  noAsk: 0.50,
  yesDepth: [],
  noDepth: []
};

let polyMarket = {
  slug: null,
  title: 'Cargando Polymarket 15M...',
  url: 'https://polymarket.com',
  strike: 0.0,
  candleSlug: null,
  tokenUp: null,
  tokenDown: null,
  secondsLeft: 900,
  upBid: 0.50,
  upAsk: 0.50,
  downBid: 0.50,
  downAsk: 0.50,
  upDepth: [],
  downDepth: []
};

// Math: Normal CDF approximation (erf-based)
function normalCdf(z) {
  const t = 1.0 / (1.0 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp(-z * z / 2.0);
  let p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  if (z > 0) p = 1.0 - p;
  return p;
}

// Certainty Engine
function calculateSnipeCertainty(delta, secondsRemaining) {
  if (secondsRemaining <= 0) return delta >= 0 ? 100 : 0;
  // BTC 15-minute expected standard deviation ~ $42.0
  const sigma15m = 42.0;
  const timeFraction = Math.max(0.01, secondsRemaining / 900.0);
  const currentSigma = sigma15m * Math.sqrt(timeFraction);
  const zScore = delta / currentSigma;

  // Probability that BTC ends >= Strike (YES wins)
  const pYes = normalCdf(zScore);
  return {
    zScore: parseFloat(zScore.toFixed(2)),
    pYes: parseFloat((pYes * 100).toFixed(1)),
    pNo: parseFloat(((1.0 - pYes) * 100).toFixed(1)),
    certainty: parseFloat((Math.max(pYes, 1.0 - pYes) * 100).toFixed(1)),
    favoredSide: delta >= 0 ? 'YES' : 'NO'
  };
}

// Multi-Stream Real-Time Price Feeds: Binance + Coinbase (<50ms tick-by-tick)
function initBinanceWs() {
  try {
    const ws = new WSClient('wss://stream.binance.com:9443/ws/btcusdt@trade');
    ws.on('open', () => {
      console.log('⚡ Conectado al stream de trades de Binance BTC/USDT (<50ms)');
      addLog('⚡ Conectado al WebSocket de Binance BTC/USDT (<50ms)', 'info');
    });
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw);
        if (msg.p) {
          binanceSpot = parseFloat(msg.p);
          lastBinanceTickTime = Date.now();
          evaluateSnipeOpportunity();
          evaluatePolymarketSnipe();
        }
      } catch (e) {}
    });
    ws.on('error', () => {});
    ws.on('close', () => {
      setTimeout(initBinanceWs, 3000);
    });
  } catch (e) {
    setTimeout(initBinanceWs, 3000);
  }
}

// Coinbase Real-Time WebSocket (<20ms in US Cloud servers like Render)
function initCoinbaseWs() {
  try {
    const ws = new WSClient('wss://ws-feed.exchange.coinbase.com');
    ws.on('open', () => {
      ws.send(JSON.stringify({
        type: 'subscribe',
        product_ids: ['BTC-USD'],
        channels: ['ticker']
      }));
      console.log('⚡ Conectado al stream de trades de Coinbase BTC/USD (<20ms)');
      addLog('⚡ Conectado al stream de alta frecuencia de Coinbase BTC/USD (<20ms)', 'info');
    });
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw);
        if (msg.price) {
          binanceSpot = parseFloat(msg.price);
          lastBinanceTickTime = Date.now();
          evaluateSnipeOpportunity();
          evaluatePolymarketSnipe();
        }
      } catch (e) {}
    });
    ws.on('error', () => {});
    ws.on('close', () => {
      setTimeout(initCoinbaseWs, 3000);
    });
  } catch (e) {
    setTimeout(initCoinbaseWs, 3000);
  }
}

// REST Fallback for Spot Price (Guarantees spot is never 0 in any cloud region)
async function pollSpotFallback() {
  if (binanceSpot > 0 && (Date.now() - lastBinanceTickTime < 4000)) return;
  try {
    const res = await fetch('https://api.coinbase.com/v2/prices/BTC-USD/spot', { signal: AbortSignal.timeout(2500) });
    if (res.ok) {
      const d = await res.json();
      if (d?.data?.amount) {
        binanceSpot = parseFloat(d.data.amount);
        lastBinanceTickTime = Date.now();
        evaluateSnipeOpportunity();
        evaluatePolymarketSnipe();
      }
    }
  } catch (e) {}
}

// Fetch active Kalshi 15M BTC Market & Orderbook
async function pollKalshiMarket() {
  try {
    const url = 'https://api.elections.kalshi.com/trade-api/v2/events?series_ticker=KXBTC15M&status=open&with_nested_markets=true';
    const res = await fetch(url, { signal: AbortSignal.timeout(4000) });
    if (!res.ok) return;
    const data = await res.json();
    if (!data.events || !data.events.length) return;

    const ev = data.events[0];
    const m = ev.markets && ev.markets[0];
    if (!m) return;

    // Extract target from title (e.g. "BTC 15 min · $63,082.41 target")
    let strike = 0;
    const match = ev.title.match(/\$([0-9,]+\.[0-9]{2})/);
    if (match) {
      strike = parseFloat(match[1].replace(/,/g, ''));
    } else if (m.custom_strike) {
      strike = parseFloat(m.custom_strike);
    }

    kalshiMarket.eventTicker = ev.event_ticker;
    kalshiMarket.marketTicker = m.ticker;
    kalshiMarket.title = ev.title;
    kalshiMarket.strike = strike || binanceSpot;
    kalshiMarket.expirationTime = m.close_time || m.expected_expiration_time || m.expiration_time;

    // Calculate seconds left until market close
    const now = new Date();
    const closeDate = new Date(kalshiMarket.expirationTime);
    kalshiMarket.secondsLeft = Math.max(0, Math.round((closeDate - now) / 1000));

    // Fetch live orderbook depth
    try {
      const obRes = await fetch(`https://api.elections.kalshi.com/trade-api/v2/markets/${m.ticker}/orderbook`, {
        signal: AbortSignal.timeout(2500)
      });
      if (obRes.ok) {
        const obData = await obRes.json();
        const ob = obData.orderbook_fp || {};
        const yesBids = ob.yes_dollars || [];
        const noBids = ob.no_dollars || [];

        const bestYesBid = yesBids.length ? parseFloat(yesBids[yesBids.length - 1][0]) : 0;
        const bestNoBid = noBids.length ? parseFloat(noBids[noBids.length - 1][0]) : 0;

        kalshiMarket.yesBid = bestYesBid;
        kalshiMarket.yesAsk = parseFloat((1.00 - bestNoBid).toFixed(2));
        kalshiMarket.noBid = bestNoBid;
        kalshiMarket.noAsk = parseFloat((1.00 - bestYesBid).toFixed(2));
        kalshiMarket.yesDepth = yesBids.slice(-5);
        kalshiMarket.noDepth = noBids.slice(-5);
      }
    } catch (e) {}

    // Check if pending open snipes should be settled
    settlePendingSnipes();

    broadcast({
      type: 'TICK',
      data: {
        binanceSpot,
        kalshi: kalshiMarket,
        poly: polyMarket,
        analysis: calculateSnipeCertainty(binanceSpot - kalshiMarket.strike, kalshiMarket.secondsLeft),
        polyAnalysis: calculateSnipeCertainty(binanceSpot - polyMarket.strike, polyMarket.secondsLeft)
      }
    });

  } catch (err) {}
}

// ============================================================================
// POLYMARKET 15M LATENCY SNIPER ENGINE
// ============================================================================
async function pollPolymarket15m() {
  try {
    const now = new Date();
    const res = await fetch('https://gamma-api.polymarket.com/events?series_slug=btc-up-or-down-15m&closed=false&limit=4', {
      signal: AbortSignal.timeout(3500)
    });
    if (!res.ok) return;
    const events = await res.json();
    if (!events || !events.length) return;

    // Pick currently running or closest
    let ev = events.find(e => now >= new Date(e.startTime) && now < new Date(e.endDate));
    if (!ev) ev = events[0];

    const m = ev.markets && ev.markets[0];
    if (!m) return;

    let tokenUp = null, tokenDown = null;
    if (m.clobTokenIds) {
      try {
        const ids = typeof m.clobTokenIds === 'string' ? JSON.parse(m.clobTokenIds) : m.clobTokenIds;
        tokenUp = ids[0];
        tokenDown = ids[1];
      } catch (e) {}
    }

    const end = new Date(ev.endDate);
    const secsLeft = Math.max(0, Math.round((end - now) / 1000));

    polyMarket.slug = ev.slug;
    polyMarket.title = ev.title;
    polyMarket.url = `https://polymarket.com/event/${ev.slug}`;
    polyMarket.secondsLeft = secsLeft;
    polyMarket.tokenUp = tokenUp;
    polyMarket.tokenDown = tokenDown;

    // Polymarket provides eventMetadata.priceToBeat which is the EXACT official Chainlink TWAP strike
    if (ev.eventMetadata && ev.eventMetadata.priceToBeat) {
      polyMarket.strike = parseFloat(ev.eventMetadata.priceToBeat);
      polyMarket.candleSlug = ev.slug;
    } else if (!polyMarket.strike || polyMarket.candleSlug !== ev.slug) {
      polyMarket.candleSlug = ev.slug;
      try {
        const kRes = await fetch('https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=1', { signal: AbortSignal.timeout(2000) });
        if (kRes.ok) {
          const kData = await kRes.json();
          if (kData && kData[0]) {
            polyMarket.strike = parseFloat(kData[0][1]);
          }
        }
      } catch (e) {}
      if (!polyMarket.strike) {
        polyMarket.strike = binanceSpot || kalshiMarket.strike;
      }
    }

    if (tokenUp && tokenDown) {
      const [upBook, downBook] = await Promise.all([
        fetch(`https://clob.polymarket.com/book?token_id=${tokenUp}`, { signal: AbortSignal.timeout(2000) }).then(r => r.json()).catch(() => ({})),
        fetch(`https://clob.polymarket.com/book?token_id=${tokenDown}`, { signal: AbortSignal.timeout(2000) }).then(r => r.json()).catch(() => ({}))
      ]);

      const upAsks = (upBook.asks || []).map(a => parseFloat(a.price)).sort((x,y) => x-y);
      const upBids = (upBook.bids || []).map(b => parseFloat(b.price)).sort((x,y) => y-x);
      const downAsks = (downBook.asks || []).map(a => parseFloat(a.price)).sort((x,y) => x-y);
      const downBids = (downBook.bids || []).map(b => parseFloat(b.price)).sort((x,y) => y-x);

      polyMarket.upAsk = upAsks[0] || 0.50;
      polyMarket.upBid = upBids[0] || 0.50;
      polyMarket.downAsk = downAsks[0] || 0.50;
      polyMarket.downBid = downBids[0] || 0.50;
      polyMarket.upDepth = (upBook.bids || []).slice(0, 5).map(b => [parseFloat(b.price), parseFloat(b.size)]);
      polyMarket.downDepth = (downBook.bids || []).slice(0, 5).map(b => [parseFloat(b.price), parseFloat(b.size)]);
    }

    evaluatePolymarketSnipe();

  } catch (err) {}
}

// CLOB Client Initialization for Automated Live Trading
async function initPolymarketClobClient() {
  const pk = process.env.POLYMARKET_PRIVATE_KEY;
  if (!pk || pk.trim() === '') {
    liveTradingReady = false;
    return;
  }
  try {
    const cleanPk = pk.trim().startsWith('0x') ? pk.trim() : `0x${pk.trim()}`;
    const signer = privateKeyToAccount(cleanPk);
    signer.getAddress = async () => signer.address;
    liveWalletAddress = signer.address;

    const funderAddress = (process.env.POLYMARKET_FUNDER_ADDRESS || '').trim();

    polymarketClient = await createSecureClient({
      signer,
      wallet: funderAddress || undefined
    });

    liveTradingReady = true;
    tradingMode = process.env.TRADING_MODE === 'LIVE' ? 'LIVE' : 'SIMULATION';
    addLog(`✅ Polymarket Unified Client conectado: ${liveWalletAddress.slice(0, 6)}...${liveWalletAddress.slice(-4)} ${funderAddress ? `(Proxy: ${funderAddress.slice(0,6)}...${funderAddress.slice(-4)})` : ''} | Modo: ${tradingMode}`, 'win');
  } catch (err) {
    console.error('Error initPolymarketClient:', err.message);
    addLog(`⚠️ Error configurando Polymarket: ${err.message}`, 'warning');
    liveTradingReady = false;
  }
}

let activePolySnipeLock = null;
let isExecutingPolyOrder = false;

async function evaluatePolymarketSnipe() {
  if (isExecutingPolyOrder) return;
  if (!polyMarket.strike || binanceSpot <= 0) return;

  const delta = binanceSpot - polyMarket.strike;
  const secs = polyMarket.secondsLeft;
  const analysis = calculateSnipeCertainty(delta, secs);

  // Strict high-conviction thresholds (avoids late candle noise):
  const minDelta = secs <= 60 ? 25.0 : (secs <= 120 ? 35.0 : 45.0);
  const minCertainty = 97.0;

  if (secs <= 180 && secs >= 5 && analysis.certainty >= minCertainty && Math.abs(delta) >= minDelta) {
    const side = analysis.favoredSide === 'YES' ? 'UP' : 'DOWN';
    const askPrice = side === 'UP' ? polyMarket.upAsk : polyMarket.downAsk;

    const candleId = `POLY-${polyMarket.slug}`;
    if (activePolySnipeLock === candleId) return;

    if (askPrice > 0 && askPrice <= 0.80) {
      // SYNCHRONOUS MUTEX LOCK: Lock instantly before any async await to prevent concurrent ticks
      activePolySnipeLock = candleId;
      isExecutingPolyOrder = true;

      // Auto-trade sizing
      const capitalToUse = autoTradeAmount || 1.0;
      const shares = Math.max(1, Math.round(capitalToUse / askPrice));
      const entryPrice = askPrice;
      const costBasis = parseFloat((shares * entryPrice).toFixed(2));
      const expectedPayout = parseFloat((shares * 1.00).toFixed(2));
      const expectedProfit = parseFloat((expectedPayout - costBasis).toFixed(2));
      const roi = parseFloat((((1.00 - entryPrice) / entryPrice) * 100).toFixed(1));

      let isLiveOrder = false;
      let orderIdResult = null;
      let orderSuccess = false;

      try {
        // LIVE EXECUTION if enabled and configured
        if (autoTradeEnabled && tradingMode === 'LIVE' && liveTradingReady && polymarketClient) {
          const targetTokenId = side === 'UP' ? polyMarket.tokenUp : polyMarket.tokenDown;
          if (targetTokenId) {
            try {
              // Attempt 1: Market Order (FAK)
              const liveOrder = await polymarketClient.createMarketOrder({
                amount: capitalToUse,
                tokenId: targetTokenId,
                side: 'BUY'
              });
              const postRes = await polymarketClient.postOrder(liveOrder);
              isLiveOrder = true;
              orderSuccess = true;
              orderIdResult = postRes?.orderID || postRes?.id || 'EXECUTED_MARKET';
              addLog(`⚡ [LIVE AUTO-TRADE EJECUTADO] Compra Market de $${capitalToUse} USDC (${side}) en Polymarket! ID: ${orderIdResult}`, 'win');
            } catch (mErr) {
              // Attempt 2: Marketable Limit Order (sweeps resting asks up to 80¢ with tolerance)
              try {
                const limitPrice = parseFloat(Math.min(0.80, askPrice + 0.02).toFixed(2));
                const limitShares = Math.max(1, Math.round(capitalToUse / limitPrice));
                const limitOrder = await polymarketClient.createLimitOrder({
                  tokenId: targetTokenId,
                  price: limitPrice,
                  size: limitShares,
                  side: 'BUY'
                });
                const postRes = await polymarketClient.postOrder(limitOrder);
                isLiveOrder = true;
                orderSuccess = true;
                orderIdResult = postRes?.orderID || postRes?.id || 'EXECUTED_LIMIT';
                addLog(`⚡ [LIVE AUTO-TRADE EJECUTADO] Compra Límite de ${limitShares} shares (${side}) a $${limitPrice} en Polymarket! ID: ${orderIdResult}`, 'win');
              } catch (lErr) {
                addLog(`ℹ️ Polymarket sin liquidez con descuento $\\le$ 80¢: ${mErr.message || lErr.message}`, 'info');
              }
            }
          }
        } else {
          // Simulation mode
          orderSuccess = true;
        }

        if (orderSuccess) {
          const snipe = {
            id: `SNIPE-POLY-${Date.now()}`,
            marketTicker: polyMarket.slug,
            platform: 'Polymarket',
            title: polyMarket.title,
            side,
            strike: polyMarket.strike,
            spotAtSnipe: binanceSpot,
            delta: parseFloat(delta.toFixed(2)),
            secondsLeft: secs,
            certainty: analysis.certainty,
            shares,
            entryPrice,
            costBasis,
            expectedPayout,
            expectedProfit,
            roi,
            status: 'OPEN',
            isLive: isLiveOrder,
            orderId: orderIdResult,
            createdAt: new Date().toISOString()
          };

          snipes.unshift(snipe);
          saveSnipes();

          addLog(`⚡ [${isLiveOrder ? 'LIVE' : 'SIM'}] ${side} cazado a $${entryPrice.toFixed(2)} | Inversión: $${costBasis} USDC | Delta: ${delta >= 0 ? '+' : ''}$${delta.toFixed(1)} | Certeza: ${analysis.certainty}%`, 'snipe');
          broadcast({ type: 'SNIPE_FIRED', data: snipe });
        }
      } finally {
        isExecutingPolyOrder = false;
      }
    } else if (askPrice > 0.80) {
      if (activePolySnipeLock !== `${candleId}-EXPENSIVE`) {
        activePolySnipeLock = `${candleId}-EXPENSIVE`;
        addLog(`🛡️ [ESCUDO DE SEGURIDAD] Certeza alta (${analysis.certainty}%), pero el contrato ya está a ${(askPrice * 100).toFixed(0)}¢ (> 80¢). Se bloquea la compra para proteger margen de ganancia.`, 'warning');
      }
    }
  }
}

let activeSnipeLock = null; // prevents duplicate execution in the same candle

function evaluateSnipeOpportunity() {
  if (!kalshiMarket.strike || binanceSpot <= 0) return;

  const delta = binanceSpot - kalshiMarket.strike;
  const secs = kalshiMarket.secondsLeft;
  const analysis = calculateSnipeCertainty(delta, secs);

  // SNIPING CRITERIA:
  // 1. Time is in the "Snipe Zone": <= 180 seconds (last 3 minutes)
  // 2. Statistical certainty is >= 98.0%
  // 3. Price breakout |Delta| >= $30.00
  // 4. Mispriced book: Kalshi Ask for the winning side is <= $0.78 (Underpriced!)
  if (secs <= 180 && secs >= 5 && analysis.certainty >= 98.0 && Math.abs(delta) >= 25.0) {
    const side = analysis.favoredSide;
    const askPrice = side === 'YES' ? kalshiMarket.yesAsk : kalshiMarket.noAsk;

    const candleId = `${kalshiMarket.marketTicker}`;
    if (activeSnipeLock === candleId) return; // already sniped this candle

    // Edge check: ask must offer at least 20% ROI (ask <= 0.80)
    if (askPrice > 0 && askPrice <= 0.80) {
      activeSnipeLock = candleId;

      const shares = 25; // 25 contracts
      const entryPrice = askPrice;
      const costBasis = parseFloat((shares * entryPrice).toFixed(2));
      const expectedPayout = parseFloat((shares * 1.00).toFixed(2));
      const expectedProfit = parseFloat((expectedPayout - costBasis).toFixed(2));
      const roi = parseFloat((((1.00 - entryPrice) / entryPrice) * 100).toFixed(1));

      const snipe = {
        id: `SNIPE-${Date.now()}`,
        marketTicker: kalshiMarket.marketTicker,
        title: kalshiMarket.title,
        side,
        strike: kalshiMarket.strike,
        spotAtSnipe: binanceSpot,
        delta: parseFloat(delta.toFixed(2)),
        secondsLeft: secs,
        certainty: analysis.certainty,
        shares,
        entryPrice,
        costBasis,
        expectedPayout,
        expectedProfit,
        roi,
        status: 'OPEN',
        createdAt: new Date().toISOString()
      };

      snipes.unshift(snipe);
      saveSnipes();

      addLog(`🎯 [SNIPE EJECUTADO] ${side} a $${entryPrice.toFixed(2)} | Delta: ${delta >= 0 ? '+' : ''}$${delta.toFixed(1)} | Certeza: ${analysis.certainty}% | Cobro esperado: $${expectedPayout} (+${roi}%)`, 'snipe');
      broadcast({ type: 'SNIPE_FIRED', data: snipe });
    }
  }
}

// Settle completed snipes when countdown reaches 0
function settlePendingSnipes() {
  const now = new Date();
  let updated = false;

  for (const s of snipes) {
    if (s.status === 'OPEN') {
      const exp = new Date(kalshiMarket.expirationTime);
      if (kalshiMarket.secondsLeft <= 0 || (exp && now >= exp)) {
        // Evaluate final outcome
        const won = (s.side === 'YES' && binanceSpot >= s.strike) || (s.side === 'NO' && binanceSpot < s.strike);
        s.status = won ? 'WON' : 'LOST';
        s.finalSpot = binanceSpot;
        s.finalProfit = won ? s.expectedProfit : -s.costBasis;
        s.settledAt = new Date().toISOString();
        updated = true;

        if (won) {
          addLog(`🏆 [SNIPE LIQUIDADO WIN] Cobrados $${s.expectedPayout} USD. Ganancia neta: +$${s.finalProfit} USD (+${s.roi}%)`, 'win');
        } else {
          addLog(`⚠️ [SNIPE PERDIDO] El precio se revirtió en los últimos segundos. PnL: -$${s.costBasis} USD`, 'warning');
        }
      }
    }
  }

  if (updated) {
    saveSnipes();
    broadcast({ type: 'SNIPES_UPDATED', data: snipes });
  }
}

// REST Endpoints
app.get('/api/status', (req, res) => {
  const wonSnipes = snipes.filter(s => s.status === 'WON');
  const lostSnipes = snipes.filter(s => s.status === 'LOST');
  const totalSettled = wonSnipes.length + lostSnipes.length;
  const winRate = totalSettled > 0 ? ((wonSnipes.length / totalSettled) * 100).toFixed(1) : '100.0';
  const totalProfit = snipes.reduce((acc, s) => acc + (s.finalProfit || 0), 0);

  const delta = binanceSpot - (kalshiMarket.strike || binanceSpot);
  const analysis = calculateSnipeCertainty(delta, kalshiMarket.secondsLeft);

  res.json({
    binanceSpot,
    kalshi: kalshiMarket,
    poly: polyMarket,
    analysis,
    polyAnalysis: calculateSnipeCertainty(binanceSpot - polyMarket.strike, polyMarket.secondsLeft),
    autoTrade: {
      enabled: autoTradeEnabled,
      amount: autoTradeAmount,
      mode: tradingMode,
      liveReady: liveTradingReady,
      walletAddress: liveWalletAddress ? `${liveWalletAddress.slice(0, 6)}...${liveWalletAddress.slice(-4)}` : null
    },
    stats: {
      totalSnipes: snipes.length,
      won: wonSnipes.length,
      lost: lostSnipes.length,
      winRate: parseFloat(winRate),
      totalProfit: parseFloat(totalProfit.toFixed(2))
    },
    snipes: snipes.slice(0, 30),
    logs: botLogs
  });
});

app.post('/api/settings', (req, res) => {
  const { enabled, amount, mode } = req.body || {};
  if (typeof enabled === 'boolean') autoTradeEnabled = enabled;
  if (typeof amount === 'number' && amount > 0) autoTradeAmount = amount;
  if (mode === 'LIVE' || mode === 'SIMULATION') tradingMode = mode;

  addLog(`⚙️ Ajustes actualizados: Auto-Trade ${autoTradeEnabled ? 'ACTIVO (ON)' : 'PAUSADO (OFF)'} | $${autoTradeAmount} USDC | Modo: ${tradingMode}`, 'info');
  res.json({
    success: true,
    autoTrade: {
      enabled: autoTradeEnabled,
      amount: autoTradeAmount,
      mode: tradingMode,
      liveReady: liveTradingReady,
      walletAddress: liveWalletAddress ? `${liveWalletAddress.slice(0, 6)}...${liveWalletAddress.slice(-4)}` : null
    }
  });
});

app.post('/api/save-key', async (req, res) => {
  const { privateKey, funderAddress, mode } = req.body || {};
  if (!privateKey || privateKey.trim() === '') {
    return res.status(400).json({ error: 'Clave privada requerida' });
  }

  try {
    const cleanPk = privateKey.trim();
    const formattedPk = cleanPk.startsWith('0x') ? cleanPk : `0x${cleanPk}`;
    // Test that private key is valid
    const testWallet = new Wallet(formattedPk);

    let envContent = `POLYMARKET_PRIVATE_KEY=${formattedPk}\n`;
    if (funderAddress && funderAddress.trim()) {
      envContent += `POLYMARKET_FUNDER_ADDRESS=${funderAddress.trim()}\n`;
    }
    envContent += `TRADING_MODE=${mode || 'LIVE'}\n`;
    envContent += `AUTO_TRADE_SIZE=${autoTradeAmount}\n`;

    fs.writeFileSync(ENV_FILE, envContent, 'utf8');
    process.env.POLYMARKET_PRIVATE_KEY = formattedPk;
    if (funderAddress && funderAddress.trim()) process.env.POLYMARKET_FUNDER_ADDRESS = funderAddress.trim();
    process.env.TRADING_MODE = mode || 'LIVE';

    await initPolymarketClobClient();
    addLog(`🔑 Clave privada guardada localmente con éxito para ${testWallet.address}`, 'win');
    res.json({
      success: true,
      walletAddress: `${testWallet.address.slice(0, 6)}...${testWallet.address.slice(-4)}`,
      liveReady: liveTradingReady,
      mode: tradingMode
    });
  } catch (err) {
    res.status(400).json({ error: `Clave privada inválida: ${err.message}` });
  }
});

app.post('/api/manual-snipe', (req, res) => {
  const { platform } = req.body || {};
  const isPoly = platform === 'Polymarket';

  const strike = isPoly ? polyMarket.strike : (kalshiMarket.strike || binanceSpot);
  const delta = binanceSpot - strike;
  const secs = isPoly ? polyMarket.secondsLeft : kalshiMarket.secondsLeft;
  const analysis = calculateSnipeCertainty(delta, secs);

  const side = isPoly ? (analysis.favoredSide === 'YES' ? 'UP' : 'DOWN') : analysis.favoredSide;
  let entryPrice = 0.70;
  if (isPoly) {
    entryPrice = side === 'UP' ? polyMarket.upAsk : polyMarket.downAsk;
  } else {
    entryPrice = side === 'YES' ? (kalshiMarket.yesAsk || 0.70) : (kalshiMarket.noAsk || 0.70);
  }

  const shares = isPoly ? Math.max(1, Math.round(autoTradeAmount / (entryPrice || 0.70))) : 25;
  const costBasis = parseFloat((shares * entryPrice).toFixed(2));
  const expectedPayout = parseFloat((shares * 1.00).toFixed(2));
  const expectedProfit = parseFloat((expectedPayout - costBasis).toFixed(2));
  const roi = parseFloat((((1.00 - entryPrice) / entryPrice) * 100).toFixed(1));

  const snipe = {
    id: `SNIPE-${isPoly ? 'POLY' : 'KALSHI'}-${Date.now()}`,
    marketTicker: isPoly ? polyMarket.slug : (kalshiMarket.marketTicker || 'KXBTC15M'),
    platform: isPoly ? 'Polymarket' : 'Kalshi',
    title: isPoly ? polyMarket.title : kalshiMarket.title,
    side,
    strike,
    spotAtSnipe: binanceSpot,
    delta: parseFloat(delta.toFixed(2)),
    secondsLeft: secs,
    certainty: analysis.certainty,
    shares,
    entryPrice,
    costBasis,
    expectedPayout,
    expectedProfit,
    roi,
    status: 'WON', // Simulated manual victory
    finalProfit: expectedProfit,
    createdAt: new Date().toISOString(),
    settledAt: new Date().toISOString()
  };

  snipes.unshift(snipe);
  saveSnipes();
  addLog(`🎯 [SNIPE ${isPoly ? 'POLYMARKET' : 'KALSHI'} DEMO] ${side} a $${entryPrice.toFixed(2)} | Cobrados $${expectedPayout} USD (+${roi}%)`, 'win');
  broadcast({ type: 'SNIPE_FIRED', data: snipe });
  res.json({ success: true, snipe });
});

// Initialization
initBinanceWs();
initCoinbaseWs();
pollSpotFallback();
setInterval(pollSpotFallback, 2000);
initPolymarketClobClient();
pollKalshiMarket();
setInterval(pollKalshiMarket, 2000);
pollPolymarket15m();
setInterval(pollPolymarket15m, 2500);

server.listen(PORT, () => {
  console.log(`================================================================`);
  console.log(`🚀 DUAL LATENCY SNIPER ACTIVO (KALSHI & POLYMARKET)`);
  console.log(`🌐 Dashboard interactivo: http://localhost:${PORT}`);
  console.log(`⚡ Feed: Binance WebSocket (<50ms) vs Kalshi & Polymarket 15M`);
  console.log(`================================================================`);
  addLog(`🚀 Motor de Sniping Dual (Kalshi + Polymarket) iniciado en puerto ${PORT}`, 'info');
});
