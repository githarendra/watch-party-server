const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');
const axios = require('axios'); // ✅ NEW: For fetching from mirrors

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

// Helper: Extract Video ID from any YouTube URL
const getVideoId = (url) => {
    const regex = /(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([^"&?\/\s]{11})/;
    const match = url.match(regex);
    return match ? match[1] : null;
};

// 📺 YOUTUBE PROXY ROUTE (Invidious Method)
app.get('/youtube', async (req, res) => {
    const videoUrl = req.query.url;
    const videoId = getVideoId(videoUrl);

    if(!videoId) return res.status(400).send('Invalid YouTube URL');

    // List of reliable public instances (Mirrors)
    const instances = [
        "https://inv.tux.pizza",
        "https://vid.puffyan.us",
        "https://invidious.projectsegfau.lt",
        "https://yt.artemislena.eu"
    ];

    console.log(`🎥 Fetching YouTube ID: ${videoId}`);

    // Loop through instances until one works
    for (const instance of instances) {
        try {
            // "itag=18" is standard 360p MP4 (Video+Audio combined)
            const directStreamUrl = `${instance}/latest_version?id=${videoId}&itag=18`;
            
            console.log(`🔄 Trying mirror: ${instance}`);

            // Fetch the stream from the mirror
            const response = await axios({
                method: 'get',
                url: directStreamUrl,
                responseType: 'stream',
                timeout: 10000 // 10s timeout
            });

            // Set headers
            res.header('Content-Type', 'video/mp4');
            res.header('Access-Control-Allow-Origin', '*');
            
            // Pipe the clean stream to your frontend
            response.data.pipe(res);
            return; // ✅ Success, stop the loop

        } catch (err) {
            console.log(`⚠️ Mirror ${instance} failed, trying next...`);
        }
    }

    // If all mirrors fail
    res.status(500).send("All mirrors busy. Try again.");
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
