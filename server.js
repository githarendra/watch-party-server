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
  transports: ['polling', 'websocket'] 
});

// Single Source of Truth
const rooms = {}; // { roomId: { hostSocket, hostName, users: [] } }

const broadcastToHost = (roomId) => {
    const room = rooms[roomId];
    if (room && room.hostSocket) {
        // Send the latest user list to the VALID host socket
        io.to(room.hostSocket).emit('update-user-list', room.users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ HOST REGISTERS (Critical for Dashboard)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // ✅ FORCE UPDATE HOST SOCKET ID
      rooms[roomId].hostSocket = socket.id; 
      rooms[roomId].hostName = username;
      
      console.log(`👑 Host Registered: ${username} (ID: ${socket.id})`);

      // 1. Tell viewers the room name
      socket.to(roomId).emit('host-name-update', username);
      
      // 2. Send the Host the current viewer list immediately
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);

    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    // Remove duplicates
    rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socket.id);
    
    // Add Viewer
    rooms[roomId].users.push({ socketId: socket.id, username, status: 'Joining...' });

    // ✅ UPDATE HOST DASHBOARD
    broadcastToHost(roomId);
    
    // Connect Peer
    socket.to(roomId).emit('user-connected', userId);

    // Send Host Name to this new viewer
    if (rooms[roomId].hostName) {
        socket.emit('host-name-update', rooms[roomId].hostName);
    }
  });

  // 3️⃣ HOST STARTS STREAM
  socket.on('host-started-stream', ({ roomId, username }) => {
    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    rooms[roomId].hostSocket = socket.id; // Confirm ID again
    rooms[roomId].hostName = username;
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
  });

  // 4️⃣ VIEWER STATUS UPDATE (Play/Pause)
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              // ✅ FORCE DASHBOARD UPDATE
              broadcastToHost(roomId);
          }
      }
  });

  // 5️⃣ SYNC HANDSHAKE
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId] && rooms[roomId].hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

  socket.on('video-sync', (data) => {
    if (data.targetSocketId) {
        io.to(data.targetSocketId).emit('video-sync', data);
    } else {
        socket.to(data.roomId).emit('video-sync', data);
    }
  });

  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));

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
      // Find room this socket belonged to
      for (const roomId in rooms) {
          const room = rooms[roomId];
          
          // If Host Disconnects
          if (room.hostSocket === socket.id) {
              // Don't delete room immediately (host might refresh), just notify stop
              // socket.to(roomId).emit('broadcast-stopped'); 
              break;
          }

          // If Viewer Disconnects
          const userIndex = room.users.findIndex(u => u.socketId === socket.id);
          if (userIndex !== -1) {
              room.users.splice(userIndex, 1);
              broadcastToHost(roomId); // Update Host Dashboard
              break;
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
