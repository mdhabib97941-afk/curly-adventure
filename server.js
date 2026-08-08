const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3001;
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.get('/', (req, res) => res.sendFile('index.html', { root: __dirname }));

app.get('/api/symbols', async (req, res) => {
  try {
    const response = await axios.get('https://fapi.binance.com/fapi/v1/exchangeInfo');
    const symbols = response.data.symbols
      .filter(s => s.quoteAsset === 'USDT' && s.status === 'TRADING' && s.contractType === 'PERPETUAL')
      .map(s => s.symbol);
    res.json(symbols);
  } catch (error) {
    console.error('Error fetching symbols:', error.message);
    res.status(500).json({ error: 'Failed to fetch symbols' });
  }
});

app.get('/api/whale-data', async (req, res) => {
  const { symbol, period = '1h' } = req.query;
  
  if (!symbol) {
    return res.status(400).json({ error: 'Symbol is required' });
  }

  try {
    const [accountsRes, positionsRes] = await Promise.all([
      axios.get(`https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=${symbol}&period=${period}&limit=1`),
      axios.get(`https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=${symbol}&period=${period}&limit=1`)
    ]);

    const accountsData = accountsRes.data.length > 0 ? accountsRes.data[0] : null;
    const positionsData = positionsRes.data.length > 0 ? positionsRes.data[0] : null;

    res.json({
      accounts: accountsData,
      positions: positionsData
    });
  } catch (error) {
    console.error(`Error fetching data for ${symbol}:`, error.message);
    res.status(500).json({ error: 'Failed to fetch whale data' });
  }
});

// Endpoint: Order Book (Short-term depth)
app.get('/api/orderbook', async (req, res) => {
  const { symbol } = req.query;
  if (!symbol) return res.status(400).json({ error: 'Symbol is required' });

  try {
    const response = await axios.get(`https://fapi.binance.com/fapi/v1/depth?symbol=${symbol}&limit=1000`);
    res.json(response.data);
  } catch (error) {
    console.error(`Error fetching orderbook for ${symbol}:`, error.message);
    res.status(500).json({ error: 'Failed to fetch orderbook data' });
  }
});

// Endpoint: Klines (Historical Data for Volume Profile)
app.get('/api/klines', async (req, res) => {
  const { symbol, interval = '1d', limit = 100 } = req.query;
  if (!symbol) return res.status(400).json({ error: 'Symbol is required' });

  try {
    const response = await axios.get(`https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`);
    res.json(response.data);
  } catch (error) {
    console.error(`Error fetching klines for ${symbol}:`, error.message);
    res.status(500).json({ error: 'Failed to fetch kline data' });
  }
});

// --- WebSocket API (Live Whale Tracking) ---
const THRESHOLD = 50000; 
let binanceWs = null;
let currentSymbol = 'BTCUSDT';

function connectBinanceWs(symbol) {
  if (binanceWs) {
    binanceWs.close();
  }
  
  const streamUrl = `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@aggTrade`;
  console.log(`Connecting to Binance WS: ${streamUrl}`);
  binanceWs = new WebSocket(streamUrl);

  binanceWs.on('message', (data) => {
    try {
      const trade = JSON.parse(data);
      if (trade.e === 'aggTrade') {
        const price = parseFloat(trade.p);
        const qty = parseFloat(trade.q);
        const value = price * qty;

        if (value >= THRESHOLD) {
          const isBuyerMaker = trade.m;
          const side = isBuyerMaker ? 'SHORT' : 'LONG';
          
          const whaleTrade = {
            id: trade.a,
            symbol: trade.s,
            price: price,
            qty: qty,
            value: value,
            side: side,
            timestamp: trade.T
          };

          wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
              client.send(JSON.stringify(whaleTrade));
            }
          });
        }
      }
    } catch(err) {
      console.error('Error parsing WS message', err);
    }
  });

  binanceWs.on('error', (err) => {
    console.error('Binance WS Error:', err.message);
  });
}

connectBinanceWs(currentSymbol);

app.post('/api/set-symbol', (req, res) => {
  const { symbol } = req.body;
  if (symbol && symbol !== currentSymbol) {
    currentSymbol = symbol;
    connectBinanceWs(symbol);
  }
  res.json({ success: true, symbol: currentSymbol });
});

server.listen(PORT, () => {
  console.log(`Whale Tracker API server running on port ${PORT}`);
});
