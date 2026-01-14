const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');
const axios = require('axios'); 

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

// 📺 YOUTUBE PROXY ROUTE (Via Cobalt API)
app.get('/youtube', async (req, res) => {
    const videoUrl = req.query.url;
    if(!videoUrl) return res.status(400).send('Invalid YouTube URL');

    // List of Cobalt instances (Reliable YouTube-to-MP4 converters)
    const instances = [
        "https://api.cobalt.tools",
        "https://co.wuk.sh",
        "https://cobalt.steamys.com",
        "https://cobalt.tools"
    ];

    console.log(`🎥 Processing YouTube URL via Cobalt: ${videoUrl}`);

    for (const instance of instances) {
        try {
            console.log(`🔍 Trying instance: ${instance}`);
            
            // 1. Ask Cobalt for a direct MP4 link
            const cobaltResponse = await axios.post(`${instance}/api/json`, {
                url: videoUrl,
                vCodec: 'h264',    // Ensure compatibility
                vQuality: '480',   // 480p is best balance of quality vs speed for streaming
                aFormat: 'mp3',
                isAudioOnly: false
            }, {
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                timeout: 5000 
            });

            const downloadUrl = cobaltResponse.data.url;

            if (downloadUrl) {
                console.log(`✅ Cobalt returned link. Streaming...`);
                
                // 2. Stream the file from the direct link
                const videoStream = await axios({
                    method: 'get',
                    url: downloadUrl,
                    responseType: 'stream',
                    timeout: 20000,
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                });

                // 3. Pipe to Frontend
                res.header('Content-Type', 'video/mp4');
                res.header('Access-Control-Allow-Origin', '*');
                videoStream.data.pipe(res);
                return; // Success!
            }

        } catch (err) {
            // console.log(`⚠️ Instance ${instance} failed: ${err.message}`);
        }
    }

    console.log("❌ All Cobalt instances failed.");
    res.status(500).send("Unable to process video. Try a different link.");
});

// --- STANDARD WATCH PARTY LOGIC ---

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
