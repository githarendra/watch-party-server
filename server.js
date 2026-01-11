const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

const CLIENT_URL = "http://localhost:5173"; 

app.use(cors({ origin: CLIENT_URL, credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/myapp", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: CLIENT_URL, methods: ["GET", "POST"], credentials: true }
});

const roomHosts = {}; 
const roomUsers = {}; 
const socketRoomMap = {}; 
const disconnectTimers = {}; 

const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    // Remove existing
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username);
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 ${username} joined ${roomId}`);
    
    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);
  });

  socket.on('host-started-stream', (roomId) => {
    if (disconnectTimers[roomId]) {
        clearTimeout(disconnectTimers[roomId]);
        delete disconnectTimers[roomId];
    }

    roomHosts[roomId] = socket.id;
    socketRoomMap[socket.id] = roomId;
    
    socket.to(roomId).emit('stream-forced-refresh');
    broadcastToHost(roomId);
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

  socket.on('video-sync', (data) => {
    socket.to(data.roomId).emit('video-sync', data);
    if (roomUsers[data.roomId]) {
        const user = roomUsers[data.roomId].find(u => u.socketId === socket.id);
        if (user) {
            user.status = data.type === 'PLAY' ? 'LIVE' : 'PAUSE';
            broadcastToHost(data.roomId);
        }
    }
  });

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      }
      broadcastToHost(roomId);
      const targetSocket = io.sockets.sockets.get(socketId);
      if(targetSocket) targetSocket.leave(roomId);
  });

  socket.on('send-message', (data) => {
    socket.to(data.roomId).emit('receive-message', data);
  });

  // --- THIS WAS MISSING ---
  socket.on('stop-broadcast', (roomId) => {
    delete roomHosts[roomId];
    socket.to(roomId).emit('broadcast-stopped');
  });
  // ------------------------

  socket.on('disconnect', () => {
    const roomId = socketRoomMap[socket.id];
    if (roomId) {
        if (roomUsers[roomId]) {
            roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
            broadcastToHost(roomId);
        }

        if (roomHosts[roomId] === socket.id) {
            console.log(`⚠️ Host left ${roomId}. Waiting 5s before ending stream...`);
            
            disconnectTimers[roomId] = setTimeout(() => {
                console.log(`🛑 5s elapsed. Ending broadcast for ${roomId}`);
                io.to(roomId).emit('broadcast-stopped'); 
                delete roomHosts[roomId];
                delete disconnectTimers[roomId];
            }, 5000); 
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

server.listen(3001, () => {
  console.log('🚀 Server running on port 3001');
});