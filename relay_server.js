// relay_server.js — วางไฟล์นี้ขึ้น Render.com (Free Web Service, ไม่ต้องใช้บัตรเครดิต)
// หน้าที่: เป็นตัวกลางส่งต่อภาพ/เสียงระหว่าง ESP32-CAM กับมือถือที่ดู
// ทั้งสองฝั่งเชื่อมต่อเข้ามาหา server นี้เอง (outbound) จึงทะลุ CGNAT ของฮอตสปอตได้

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt'); // ใหม่ - ใช้ส่งต่อ "สถานะเสียง/สถานะกล้อง" ไปให้จอ OLED บน ESP32 Dev Kit ผ่าน HiveMQ

const DEVICE_TOKEN = process.env.DEVICE_TOKEN || 'ตั้งรหัสลับของกล้องเอง'; // ต้องตรงกับใน esp32cam_camera.ino

// ---------- สะพานเชื่อม MQTT (ใหม่) ----------
// หน้าที่: ฟังข้อความ "AUDIO_STATUS:0/1" ที่กล้องส่งเข้ามาทาง WebSocket อยู่แล้ว แล้ว publish ต่อขึ้น HiveMQ
// เพื่อให้บอร์ด ESP32 Dev Kit (esp32_servo_controller.ino) ซึ่งต่อ MQTT อยู่แล้ว เอาไปโชว์บนจอ OLED ได้
// เลือกทำสะพานนี้ที่ relay server (Render.com) แทนที่จะเพิ่ม MQTT ในบอร์ดกล้องเอง เพราะบอร์ดกล้อง (ESP32-CAM)
// ใช้ RAM ใกล้เต็มอยู่แล้วจากกล้อง+I2S+WebSocket(TLS) ตัวเดียว การเพิ่มการเชื่อมต่อ TLS อีกเส้นเสี่ยงทำให้ไม่เสถียร
// ไปตั้งค่า Environment Variables บน Render.com (Dashboard > Environment) ให้ตรงกับใน esp32_servo_controller.ino:
//   MQTT_HOST, MQTT_USER, MQTT_PASS (และ MQTT_PORT ถ้าไม่ใช่ 8883 ค่าเริ่มต้น)
const MQTT_HOST = process.env.MQTT_HOST || '';
const MQTT_PORT = Number(process.env.MQTT_PORT || 8883);
const MQTT_USER = process.env.MQTT_USER || '';
const MQTT_PASS = process.env.MQTT_PASS || '';
const TOPIC_CAM_STATUS = 'securitycam/cam/status'; // online/offline ของบอร์ดกล้อง (retained)
const TOPIC_CAM_AUDIO  = 'securitycam/cam/audio';  // "1"=ไมค์/ลำโพงพร้อม "0"=มีปัญหา (retained)

let mqttBridge = null;
function connectMqttBridge() {
  if (!MQTT_HOST) {
    console.log('[MQTT bridge] ยังไม่ได้ตั้งค่า MQTT_HOST (Environment Variable) — ข้ามไปก่อน จอ OLED จะไม่เห็นสถานะเสียง');
    return;
  }
  mqttBridge = mqtt.connect(`mqtts://${MQTT_HOST}:${MQTT_PORT}`, {
    username: MQTT_USER,
    password: MQTT_PASS,
    clientId: 'relay-bridge-' + Math.random().toString(16).slice(2),
    reconnectPeriod: 3000,
    will: { topic: TOPIC_CAM_STATUS, payload: 'offline', qos: 1, retain: true },
  });
  mqttBridge.on('connect', () => console.log('[MQTT bridge] เชื่อม HiveMQ สำเร็จ'));
  mqttBridge.on('error', (e) => console.log('[MQTT bridge] error: ' + e.message));
}
function publishCamStatus(status) { // 'online' | 'offline'
  if (mqttBridge && mqttBridge.connected) mqttBridge.publish(TOPIC_CAM_STATUS, status, { retain: true });
}
function publishCamAudio(ready) { // true | false
  if (mqttBridge && mqttBridge.connected) mqttBridge.publish(TOPIC_CAM_AUDIO, ready ? '1' : '0', { retain: true });
}
connectMqttBridge();

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
          publishCamStatus('online');
        }
        return;
      }

      if (ws.isCamera) {
        // ข้อความสถานะเสียงจากกล้อง ("AUDIO_STATUS:0/1") -> ส่งต่อขึ้น MQTT ให้จอ OLED ด้วย
        // (นอกเหนือจากที่ส่งต่อให้ viewer.html ตามปกติในลูปด้านล่าง)
        if (!isBinary) {
          const text = data.toString();
          if (text.startsWith('AUDIO_STATUS:')) publishCamAudio(text.split(':')[1] === '1');
        }
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
    if (ws.isCamera) { camSocket = null; console.log('กล้องตัดการเชื่อมต่อ'); publishCamStatus('offline'); }
    else viewers.delete(ws);
  });

  if (!ws.isCamera) viewers.add(ws); // ผู้ชมทุกคนที่ยังไม่ auth เป็นกล้อง ถือว่าเป็น viewer ชั่วคราว
});

// กันเซิร์ฟเวอร์ทั้งตัวล่มจาก error ที่ไม่ได้ดักไว้จุดใดจุดหนึ่ง (Render จะไม่รีสตาร์ทให้ทันทีถ้า process ตายกลางอากาศ)
process.on('uncaughtException', (err) => console.log('uncaughtException: ' + err.message));
process.on('unhandledRejection', (err) => console.log('unhandledRejection: ' + err));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('relay server ทำงานที่พอร์ต ' + PORT));
