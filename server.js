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

// Helper: Extract Video ID
const getVideoId = (url) => {
    const regex = /(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([^"&?\/\s]{11})/;
    const match = url.match(regex);
    return match ? match[1] : null;
};

// 📺 YOUTUBE PROXY ROUTE (Using Piped API)
app.get('/youtube', async (req, res) => {
    const videoUrl = req.query.url;
    const videoId = getVideoId(videoUrl);

    if(!videoId) return res.status(400).send('Invalid YouTube URL');

    // ✅ LIST OF PIPED API INSTANCES (More stable than Invidious)
    const instances = [
        "https://pipedapi.kavin.rocks",
        "https://api.piped.privacy.com.de",
        "https://pipedapi.tokhmi.xyz",
        "https://api.piped.projectsegfau.lt",
        "https://pipedapi.moomoo.me"
    ];

    console.log(`🎥 Fetching Piped Stream for: ${videoId}`);

    for (const instance of instances) {
        try {
            console.log(`🔍 Checking API: ${instance}`);
            
            // 1. Fetch video data from Piped
            const apiUrl = `${instance}/streams/${videoId}`;
            const apiRes = await axios.get(apiUrl, { timeout: 4000 });

            // 2. Extract valid streams
            const audioStreams = apiRes.data.audioStreams;
            const videoStreams = apiRes.data.videoStreams;

            // We need a combined stream (video+audio) for simplicity, or we proxy just the video 
            // Piped separates them often, BUT usually has a 'hls' link or 'related' format.
            // Actually, simplest strategy: Find a video stream with sound, or proxy the HLS file.
            
            // Look for a standard mp4 stream first
            let chosenStream = videoStreams.find(s => s.videoOnly === false && s.format === 'MPEG-4');
            
            // If Piped only gives separate streams (common), we fallback to HLS (Master Playlist)
            // HLS (.m3u8) works natively in Safari/iOS, but Chrome needs hls.js. 
            // YOUR PLAYER USES NATIVE HTML5, so we really need a single .mp4 file.
            
            // Forcefully look for ANY stream that isn't videoOnly.
            if (!chosenStream) {
                // Some piped instances return combined streams in specific formats
                 chosenStream = videoStreams.find(s => s.videoOnly === false);
            }

            if (chosenStream) {
                console.log(`✅ Found stream at ${instance}`);
                
                const videoStream = await axios({
                    method: 'get',
                    url: chosenStream.url,
                    responseType: 'stream',
                    timeout: 20000,
                });

                res.header('Content-Type', 'video/mp4');
                res.header('Access-Control-Allow-Origin', '*');
                videoStream.data.pipe(res);
                return; 
            } else {
                // If Piped splits audio/video, we can't easily merge them on the fly without ffmpeg.
                // Fallback: Try to use the HLS link if available, but browsers might not play it.
                // Let's look for the 'hls' field in response.
                 if (apiRes.data.hls) {
                    // HLS is tricky without a special player. Let's stick to searching instances.
                    console.log(`⚠️ ${instance} only had split streams. Skipping.`);
                 }
            }

        } catch (err) {
            // console.log(`⚠️ Skipped ${instance}: ${err.message}`);
        }
    }

    console.log("❌ All Piped mirrors failed or returned split streams.");
    res.status(500).send("No compatible MP4 stream found. Try another link.");
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
