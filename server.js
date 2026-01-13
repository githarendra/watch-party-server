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
  cors: { origin: CLIENT_URL, methods: ["GET", "POST"], credentials: true },
  transports: ['polling', 'websocket'] // Fix connection stability
});

// CENTRAL STATE
const rooms = {}; // { roomId: { hostSocket, hostName, users: [] } }

const broadcastToHost = (roomId) => {
    if (rooms[roomId] && rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected:", socket.id);

  // 1️⃣ HOST REGISTERS (Immediate)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      rooms[roomId].hostSocket = socket.id;
      rooms[roomId].hostName = username;
      
      // Send name to any waiting viewers
      socket.to(roomId).emit('host-name-update', username);
      
      // Update Host Dashboard
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER REGISTERS (Decoupled from Video)
  socket.on('join-room-viewer', ({ roomId, username }) => {
      socket.join(roomId);
      
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // Add to list (prevent duplicates)
      const existing = rooms[roomId].users.find(u => u.socketId === socket.id);
      if (!existing) {
          rooms[roomId].users.push({ socketId: socket.id, username, status: 'Connecting...' });
      }

      // Update Host Count
      broadcastToHost(roomId);

      // Send Host Name to Viewer
      if (rooms[roomId].hostName) {
          socket.emit('host-name-update', rooms[roomId].hostName);
      }
  });

  // 3️⃣ VIDEO CONNECTION (PeerJS signal)
  socket.on('user-connected-video', ({ roomId, userId }) => {
      socket.to(roomId).emit('user-connected', userId);
      
      // Update status to Live
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) user.status = 'LIVE';
          broadcastToHost(roomId);
      }
  });

  // 4️⃣ SYNC REQUEST
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId] && rooms[roomId].hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

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
    if(rooms[roomId]) delete rooms[roomId].hostSocket;
    socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
      for (const roomId in rooms) {
          const room = rooms[roomId];
          if (room.hostSocket === socket.id) {
              // Host left
              delete room.hostSocket;
              break;
          }
          const userIndex = room.users.findIndex(u => u.socketId === socket.id);
          if (userIndex !== -1) {
              room.users.splice(userIndex, 1);
              broadcastToHost(roomId);
              break;
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
