const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');
const axios = require('axios'); 
const ytdl = require('@distube/ytdl-core');

const app = express();
const server = http.createServer(app);

const CLIENT_URL = "https://client-six-vert-25.vercel.app"; 

app.use(cors({ origin: CLIENT_URL, credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { 
    origin: CLIENT_URL, 
    methods: ["GET", "POST"], 
    credentials: true 
  },
  transports: ['polling', 'websocket'] 
});

// 🍪 COOKIE SETUP (For Backup Method)
let agent;
try {
    if (process.env.YOUTUBE_COOKIES) {
        agent = ytdl.createAgent(JSON.parse(process.env.YOUTUBE_COOKIES));
        console.log("✅ Cookies loaded.");
    }
} catch (e) { console.log("⚠️ Cookie error:", e.message); }

// 📺 YOUTUBE PROXY ROUTE
app.get('/youtube', async (req, res) => {
    const videoUrl = req.query.url;
    if(!videoUrl) return res.status(400).send('Invalid URL');

    console.log(`🎥 Processing: ${videoUrl}`);

    // --- STRATEGY 1: COBALT API (With Browser Headers) ---
    const cobaltInstances = [
        "https://api.cobalt.tools",
        "https://cobalt.steamys.com",
        "https://co.wuk.sh",
        "https://api.server.cobalt.tools"
    ];

    for (const instance of cobaltInstances) {
        try {
            console.log(`🔍 Trying Cobalt: ${instance}`);
            
            // 1. Get the Direct Link (Spoofing a Browser)
            const cobaltRes = await axios.post(`${instance}/api/json`, {
                url: videoUrl,
                vQuality: "480",
                filenamePattern: "classic",
                isAudioOnly: false,
                disableMetadata: true
            }, {
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', // 👈 FAKE BROWSER
                    'Origin': 'https://cobalt.tools',
                    'Referer': 'https://cobalt.tools/'
                },
                timeout: 6000
            });

            const downloadUrl = cobaltRes.data.url;

            if (downloadUrl) {
                console.log(`✅ Cobalt Success. Streaming...`);
                
                // 2. Stream the file
                const videoStream = await axios({
                    method: 'get',
                    url: downloadUrl,
                    responseType: 'stream',
                    timeout: 20000,
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                    }
                });

                res.header('Content-Type', 'video/mp4');
                res.header('Access-Control-Allow-Origin', '*');
                videoStream.data.pipe(res);
                return; // Done!
            }
        } catch (err) {
            // console.log(`⚠️ ${instance} failed.`);
        }
    }

    // --- STRATEGY 2: YTDL BACKUP (TV_EMBEDDED CLIENT) ---
    // If Cobalt fails, we try accessing YouTube as a Smart TV (Bypasses many blocks)
    try {
        console.log("⚠️ Cobalt failed. Trying YouTube TV Client...");
        
        ytdl(videoUrl, {
            agent: agent,
            clients: ['TV_EMBEDDED', 'IOS'], // 👈 Bypass logic
            quality: '18', // 360p MP4
            requestOptions: { family: 4 } // Force IPv4
        }).pipe(res);
        return;

    } catch (ytdlErr) {
        console.error("❌ All methods failed:", ytdlErr.message);
        res.status(500).send("Unable to load video. YouTube is blocking this server.");
    }
});

// --- WATCH PARTY LOGIC ---

const roomHosts = {};      
const roomUsers = {};      
const roomHostNames = {};  
const socketRoomMap = {};  

const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  socket.on('host-joined', ({ roomId, username }) => {
    socket.join(roomId);
    roomHosts[roomId] = socket.id;
    socketRoomMap[socket.id] = roomId;
    roomHostNames[roomId] = username;
    socket.to(roomId).emit('host-name', username);
    broadcastToHost(roomId);
  });

  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;
    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username && u.socketId !== socket.id);
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });
    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);
    if (roomHostNames[roomId]) socket.emit('host-name', roomHostNames[roomId]);
  });

  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) io.to(hostSocketId).emit('request-sync-from-host', socket.id);
  });

  socket.on('host-sync-data', ({ targetSocketId, time, state }) => {
      io.to(targetSocketId).emit('video-sync', { type: state, time });
  });

  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (roomUsers[roomId]) {
          const user = roomUsers[roomId].find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));
  socket.on('video-sync', (data) => socket.to(data.roomId).emit('video-sync', data));

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (roomUsers[roomId]) roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      broadcastToHost(roomId);
  });

  socket.on('stop-broadcast', (roomId) => {
    delete roomHosts[roomId];
    delete roomHostNames[roomId];
    socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
    const roomId = socketRoomMap[socket.id];
    if (roomId) {
        if (roomUsers[roomId]) {
            roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
            broadcastToHost(roomId);
        }
        if (roomHosts[roomId] === socket.id) {
            io.to(roomId).emit('broadcast-stopped'); 
            delete roomHosts[roomId];
            delete roomHostNames[roomId];
        }
        delete socketRoomMap[socket.id];
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
