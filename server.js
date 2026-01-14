const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');
const ytdl = require('@distube/ytdl-core'); // ✅ IMPORT YTDL

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

// 🍪 COOKIE SETUP (The Fix for 403 Errors)
let agent;
try {
    const cookieString = process.env.YOUTUBE_COOKIES;
    if (cookieString) {
        const cookies = JSON.parse(cookieString);
        agent = ytdl.createAgent(cookies);
        console.log("✅ YouTube Cookies loaded successfully.");
    } else {
        console.log("⚠️ No YOUTUBE_COOKIES found in environment variables.");
    }
} catch (error) {
    console.error("❌ Error parsing YOUTUBE_COOKIES:", error.message);
}

// 📺 YOUTUBE ROUTE
app.get('/youtube', (req, res) => {
    const videoUrl = req.query.url;
    if(!videoUrl) return res.status(400).send('No URL provided');

    res.header('Content-Type', 'video/mp4');
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Cross-Origin-Resource-Policy', 'cross-origin');

    try {
        // Pass the 'agent' (cookies) to prove we are human
        ytdl(videoUrl, { 
            agent: agent, 
            quality: '18' // Quality 18 = 360p (Single file, best for streaming)
        }).pipe(res);
    } catch (err) {
        console.error("YouTube Stream Error:", err);
        res.status(500).send("Stream Error");
    }
});

// --- STANDARD WATCH PARTY LOGIC BELOW ---

// Storage
const roomHosts = {};      // roomId -> hostSocketId
const roomUsers = {};      // roomId -> [ { socketId, username, status } ]
const roomHostNames = {};  // roomId -> "Harry"
const socketRoomMap = {};  // socketId -> roomId

const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ HOST JOINS
  socket.on('host-joined', ({ roomId, username }) => {
    socket.join(roomId);
    roomHosts[roomId] = socket.id;
    socketRoomMap[socket.id] = roomId;
    
    roomHostNames[roomId] = username;
    
    console.log(`👑 Host ${username} created room ${roomId}`);
    
    socket.to(roomId).emit('host-name', username);
    broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username && u.socketId !== socket.id);
    
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 Viewer ${username} joined ${roomId}`);

    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);

    if (roomHostNames[roomId]) {
        socket.emit('host-name', roomHostNames[roomId]);
    }
  });

  // 3️⃣ SYNC HANDSHAKE
  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          io.to(hostSocketId).emit('request-sync-from-host', socket.id);
      }
  });

  // 4️⃣ HOST REPLIES TO SYNC
  socket.on('host-sync-data', ({ targetSocketId, time, state }) => {
      io.to(targetSocketId).emit('video-sync', { type: state, time });
  });

  // 5️⃣ VIEWER STATUS UPDATE
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (roomUsers[roomId]) {
          const user = roomUsers[roomId].find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  socket.on('send-message', (data) => {
    socket.to(data.roomId).emit('receive-message', data);
  });

  socket.on('video-sync', (data) => {
    socket.to(data.roomId).emit('video-sync', data);
  });

  // KICK USER
  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      }
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
