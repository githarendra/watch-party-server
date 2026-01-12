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
  cors: { origin: CLIENT_URL, methods: ["GET", "POST"], credentials: true }
});

const roomHosts = {}; 
const roomUsers = {}; 
const socketRoomMap = {}; 
const roomDetails = {}; // ✅ Stores Host Name

// Helper: Send updated user list to the Host
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

    // 1. Add User to List
    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    // Remove duplicates
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username);
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

    // 2. Notify Host
    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);

    // 3. Send Host Name to the new Viewer
    if (roomDetails[roomId]) {
        socket.emit('host-name-update', roomDetails[roomId].hostName);
    }
  });

  // ✅ Host starts stream: Store Name & Map Socket
  socket.on('host-started-stream', ({ roomId, username }) => {
    roomHosts[roomId] = socket.id;
    socketRoomMap[socket.id] = roomId;
    roomDetails[roomId] = { hostName: username }; // Store Name
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
  });

  // ✅ SYNC FIX: Viewer asks for current time/state
  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          // Ask host to send data specifically to THIS viewer
          io.to(hostSocketId).emit('request-sync-from-host', socket.id);
      }
  });

  // ✅ STATUS FIX: Viewer reports "I Paused" or "I am Watching"
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (roomUsers[roomId]) {
          const user = roomUsers[roomId].find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId); // Update Host UI
          }
      }
  });

  // Chat
  socket.on('send-message', (data) => {
    socket.to(data.roomId).emit('receive-message', data);
  });

  // Sync Video
  socket.on('video-sync', (data) => {
    if (data.targetSocketId) {
        // Direct Sync (Initial Join)
        io.to(data.targetSocketId).emit('video-sync', data);
    } else {
        // Broadcast Sync (Normal)
        socket.to(data.roomId).emit('video-sync', data);
    }
  });

  // Kick User
  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked'); // Tell viewer they are kicked
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      }
      broadcastToHost(roomId);
  });

  socket.on('stop-broadcast', (roomId) => {
    delete roomHosts[roomId];
    delete roomDetails[roomId];
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
            delete roomDetails[roomId];
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
