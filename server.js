const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

app.use(cors());

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { 
    origin: "*", // ✅ Allow all origins to fix connection blocking
    methods: ["GET", "POST"], 
    credentials: true 
  },
  transports: ['websocket', 'polling'], // ✅ Try WebSocket first for speed
  pingTimeout: 60000, // ✅ Increase timeout for Render
  pingInterval: 25000
});

const roomHosts = {}; 
const roomUsers = {}; 
const socketRoomMap = {}; 

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
      
      // Update Host with current list immediately
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    // Remove duplicates
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username && u.socketId !== socket.id);
    
    // Add User (Default: Waiting)
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'Connecting...' });

    // Notify Host
    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);
  });

  // 3️⃣ NAME SYSTEM: Relay Request from Viewer -> Host
  socket.on('get-host-name', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          io.to(hostSocketId).emit('ask-host-name', socket.id);
      }
  });

  // 4️⃣ NAME SYSTEM: Relay Reply from Host -> Viewer
  socket.on('return-host-name', ({ targetSocketId, name }) => {
      io.to(targetSocketId).emit('set-host-name', name);
  });

  // 5️⃣ SYNC SYSTEM: Relay Request from Viewer -> Host
  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          io.to(hostSocketId).emit('ask-sync-data', socket.id);
      }
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
  
  socket.on('video-sync', (data) => {
    if (data.targetSocketId) {
        io.to(data.targetSocketId).emit('video-sync', data);
    } else {
        socket.to(data.roomId).emit('video-sync', data);
    }
  });

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      }
      broadcastToHost(roomId);
  });

  socket.on('stop-broadcast', (roomId) => {
    delete roomHosts[roomId];
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
        }
        delete socketRoomMap[socket.id];
    }
  });
  
  socket.on('leave-room', () => {
      const roomId = socketRoomMap[socket.id];
      if(roomId) {
        if (roomUsers[roomId]) {
            roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
            broadcastToHost(roomId);
        }
        delete socketRoomMap[socket.id];
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
