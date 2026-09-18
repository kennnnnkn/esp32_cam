// relay_server.js — วางไฟล์นี้ขึ้น Render.com (Free Web Service, ไม่ต้องใช้บัตรเครดิต)
// หน้าที่: เป็นตัวกลางส่งต่อภาพ/เสียงระหว่าง ESP32-CAM กับมือถือที่ดู
// ทั้งสองฝั่งเชื่อมต่อเข้ามาหา server นี้เอง (outbound) จึงทะลุ CGNAT ของฮอตสปอตได้

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const DEVICE_TOKEN = process.env.DEVICE_TOKEN || 'ตั้งรหัสลับของกล้องเอง'; // ต้องตรงกับใน esp32cam_camera.ino

// เก็บ viewer.html ไว้ในไฟล์เดียวกับที่ push ขึ้น GitHub/Render (โฟลเดอร์เดียวกับ relay_server.js)
// Render ให้ HTTPS มาด้วยในตัว จึงเปิดหน้านี้แล้วขอสิทธิ์ไมค์ (push-to-talk) ได้ทันที ไม่ต้องพึ่งที่โฮสต์อื่น
const VIEWER_HTML_PATH = path.join(__dirname, 'viewer.html');

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/viewer.html') {
    fs.readFile(VIEWER_HTML_PATH, (err, data) => {
      if (err) {
        res.writeHead(200);
        res.end('relay ok'); // ยังใช้ ping กันไม่ให้ server หลับได้เหมือนเดิมถ้าหาไฟล์ viewer.html ไม่เจอ
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }
  res.writeHead(200);
  res.end('relay ok'); // ใช้เป็น endpoint ให้ ESP32-CAM/UptimeRobot ping กันไม่ให้ server หลับ
});

const wss = new WebSocket.Server({ server });

let camSocket = null;
const viewers = new Set();

wss.on('connection', (ws, req) => {
  ws.isCamera = false;
  ws.authed = false;

  ws.on('error', () => { /* socket เดี่ยวมีปัญหา ไม่ให้กระทบ client อื่น */ });

  ws.on('message', (data, isBinary) => {
    try {
      // ข้อความแรกจากกล้องต้องเป็น "AUTH:token"
      if (!isBinary && data.toString().startsWith('AUTH:')) {
        const token = data.toString().split(':')[1];
        if (token === DEVICE_TOKEN) {
          ws.isCamera = true;
          ws.authed = true;
          viewers.delete(ws); // เดิมถูกนับเป็น viewer ชั่วคราวตอนต่อเข้ามา ต้องเอาออกตอนกลายเป็นกล้อง
          camSocket = ws;
          console.log('กล้องเชื่อมต่อและยืนยันตัวตนแล้ว');
        }
        return;
      }

      if (ws.isCamera) {
        // ข้อมูลจากกล้อง (ภาพ/เสียง) -> ส่งต่อให้ทุกมือถือที่ดูอยู่
        for (const viewer of viewers) {
          if (viewer.readyState === WebSocket.OPEN) {
            try { viewer.send(data, { binary: isBinary }); } catch (e) { /* ข้าม viewer ตัวนี้ ตัวอื่นยังได้รับปกติ */ }
          }
        }
      } else {
        // เป็นมือถือผู้ชม: ข้อความควบคุม (FLASH_ON, TAKE_PHOTO, TALK_START ฯลฯ)
        // หรือเสียงจากไมค์มือถือ -> ส่งต่อให้กล้อง
        if (camSocket && camSocket.readyState === WebSocket.OPEN) {
          try { camSocket.send(data, { binary: isBinary }); } catch (e) { /* กล้องรับไม่ได้ตอนนี้ ข้ามไปเฟรมถัดไป */ }
        }
      }
    } catch (e) {
      console.log('ข้อความผิดปกติ ข้ามไป: ' + e.message);
    }
  });

  ws.on('close', () => {
    if (ws.isCamera) { camSocket = null; console.log('กล้องตัดการเชื่อมต่อ'); }
    else viewers.delete(ws);
  });

  if (!ws.isCamera) viewers.add(ws); // ผู้ชมทุกคนที่ยังไม่ auth เป็นกล้อง ถือว่าเป็น viewer ชั่วคราว
});

// กันเซิร์ฟเวอร์ทั้งตัวล่มจาก error ที่ไม่ได้ดักไว้จุดใดจุดหนึ่ง (Render จะไม่รีสตาร์ทให้ทันทีถ้า process ตายกลางอากาศ)
process.on('uncaughtException', (err) => console.log('uncaughtException: ' + err.message));
process.on('unhandledRejection', (err) => console.log('unhandledRejection: ' + err));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('relay server ทำงานที่พอร์ต ' + PORT));
