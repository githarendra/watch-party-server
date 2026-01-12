const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Your Vercel App URL
const CLIENT_URL = "https://client-six-vert-25.vercel.app"; 

app.use(cors({ origin: CLIENT_URL, credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: CLIENT_URL, methods: ["GET", "POST"], credentials: true }
});

// Store State
const rooms = {}; // Structure: { roomId: { hostSocket: '...', hostName: '...', users: [] } }

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ HOST STARTS STREAM
  socket.on('host-joined', ({ roomId, username }) => {
    socket.join(roomId);
    
    // Initialize Room
    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    rooms[roomId].hostSocket = socket.id;
    rooms[roomId].hostName = username;

    console.log(`Host ${username} created room ${roomId}`);
    
    // Broadcast Host Name to anyone already there
    io.to(roomId).emit('host-name', username);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('viewer-joined', ({ roomId, username }) => {
    socket.join(roomId);

    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    // Add to User List
    const user = { socketId: socket.id, username, status: 'Joining...' };
    rooms[roomId].users.push(user);

    // Send Host Name to Viewer
    if (rooms[roomId].hostName) {
        socket.emit('host-name', rooms[roomId].hostName);
    }

    // Notify Host of new User
    if (rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
  });

  // 3️⃣ VIEWER REQUESTS SYNC (The "Fix" for autoplay)
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId] && rooms[roomId].hostSocket) {
          // Ask Host to send time info to THIS specific viewer
          io.to(rooms[roomId].hostSocket).emit('send-sync-to-new-viewer', socket.id);
      }
  });

  // 4️⃣ HOST REPLIES WITH SYNC
  socket.on('host-sync-data', ({ targetSocketId, time, isPlaying }) => {
      io.to(targetSocketId).emit('force-sync', { time, isPlaying });
  });

  // 5️⃣ STATUS UPDATES (Viewer Pauses/Plays)
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              // Tell Host
              if (rooms[roomId].hostSocket) {
                  io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
              }
          }
      }
  });

  // 6️⃣ GENERAL VIDEO SYNC (Host -> Everyone)
  socket.on('video-sync', (data) => {
      socket.to(data.roomId).emit('video-sync', data);
  });

  // 7️⃣ CHAT
  socket.on('send-message', (data) => {
      socket.to(data.roomId).emit('receive-message', data);
  });

  // 8️⃣ KICK
  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (rooms[roomId]) {
          rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socketId);
          if (rooms[roomId].hostSocket) {
              io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
          }
      }
  });

  // 9️⃣ DISCONNECT
  socket.on('disconnect', () => {
      // Find which room this socket was in
      for (const roomId in rooms) {
          const room = rooms[roomId];
          
          // If Host left
          if (room.hostSocket === socket.id) {
              io.to(roomId).emit('broadcast-stopped');
              delete rooms[roomId]; // Close room
              break;
          }

          // If Viewer left
          const userIndex = room.users.findIndex(u => u.socketId === socket.id);
          if (userIndex !== -1) {
              room.users.splice(userIndex, 1);
              if (room.hostSocket) {
                  io.to(room.hostSocket).emit('update-user-list', room.users);
              }
              break;
          }
      }
  });
});

server.listen(3001, () => {
  console.log('🚀 Server running on port 3001');
});
