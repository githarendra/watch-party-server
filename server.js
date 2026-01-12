const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

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
  transports: ['polling', 'websocket'] // ✅ Fixes connection stability
});

// Unified Room State: { roomId: { hostSocket, hostName, users: [] } }
const rooms = {}; 
const socketRoomMap = {}; 

const broadcastToHost = (roomId) => {
    if (rooms[roomId] && rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ Host Registers
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      socketRoomMap[socket.id] = roomId;

      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      rooms[roomId].hostSocket = socket.id;
      rooms[roomId].hostName = username;
      
      console.log(`🏠 Host registered: ${username} in ${roomId}`);

      // Sync existing viewers
      socket.to(roomId).emit('host-name-update', username);
      broadcastToHost(roomId);
  });

  // 2️⃣ Viewer Joins
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;

    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    // Add User (avoid duplicates)
    rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socket.id);
    rooms[roomId].users.push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 Viewer joined: ${username}`);

    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);

    // ✅ Send Host Name immediately
    if (rooms[roomId].hostName) {
        socket.emit('host-name-update', rooms[roomId].hostName);
    }
  });

  // 3️⃣ Host Starts Stream
  socket.on('host-started-stream', ({ roomId, username }) => {
    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    rooms[roomId].hostSocket = socket.id;
    rooms[roomId].hostName = username;
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
  });

  // 4️⃣ Sync Handshake
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId] && rooms[roomId].hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

  // 5️⃣ Status Update
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
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
      if (rooms[roomId]) {
          rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socketId);
          broadcastToHost(roomId);
      }
  });

  socket.on('stop-broadcast', (roomId) => {
    if (rooms[roomId]) {
        delete rooms[roomId].hostSocket;
        // Keep users/name alive in case of refresh, but notify stop
        socket.to(roomId).emit('broadcast-stopped');
    }
  });

  socket.on('disconnect', () => {
    const roomId = socketRoomMap[socket.id];
    if (roomId && rooms[roomId]) {
        // Viewer Left
        rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socket.id);
        broadcastToHost(roomId);

        // Host Left
        if (rooms[roomId].hostSocket === socket.id) {
            io.to(roomId).emit('broadcast-stopped'); 
            delete rooms[roomId].hostSocket;
        }
    }
    delete socketRoomMap[socket.id];
  });
  
  socket.on('leave-room', () => {
      const roomId = socketRoomMap[socket.id];
      if(roomId && rooms[roomId]) {
        rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socket.id);
        broadcastToHost(roomId);
      }
      delete socketRoomMap[socket.id];
  });
});

server.listen(3001, () => {
  console.log('🚀 Server running on port 3001');
});
