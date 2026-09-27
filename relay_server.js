// relay_server.js — วางไฟล์นี้ขึ้น Render.com (Free Web Service, ไม่ต้องใช้บัตรเครดิต)
// หน้าที่: เป็นตัวกลางส่งต่อภาพ/เสียงระหว่าง ESP32-CAM กับมือถือที่ดู
// ทั้งสองฝั่งเชื่อมต่อเข้ามาหา server นี้เอง (outbound) จึงทะลุ CGNAT ของฮอตสปอตได้
//
// + เพิ่ม MQTT bridge (ไม่บังคับ): อ่านข้อความ AUDIO_STATUS:/ERRCODE: ที่บอร์ดกล้องส่งเข้ามา
//   ทาง WebSocket อยู่แล้ว (เส้นทางเดิม ไม่แตะต้อง) แล้ว publish ต่อเข้า HiveMQ ให้จอ OLED บน
//   บอร์ดเซอร์โวอ่านสถานะเสียงของกล้องได้ — เลือกให้ "ที่นี่" (รันบน Render ที่มีทรัพยากร
//   เหลือเฟือ) เป็นคนต่อ MQTT แทนที่จะให้บอร์ดกล้องต่อ MQTT เองตรงๆ เพื่อไม่เพิ่ม TLS
//   session ที่ 2 บนบอร์ดกล้องซึ่งหน่วยความจำตึงอยู่แล้ว (รายละเอียดเต็มดูใน
//   esp32cam_camera.ino หัวข้อ "MQTT สถานะ")
//
//   ตั้ง environment variable ใน Render ถ้าต้องการฟีเจอร์นี้ (ไม่ตั้งก็ไม่เป็นไร วิดีโอ/เสียง/
//   ควบคุมเซอร์โวยังใช้งานได้ปกติทุกอย่าง แค่ช่อง "Audio" บนจอ OLED จะค้างที่ "?" เท่านั้น):
//     MQTT_HOST = xxxxxxxx.s1.eu.hivemq.cloud   (อันเดียวกับใน esp32_servo_controller.ino)
//     MQTT_USER = MG995                          (อันเดียวกับใน esp32_servo_controller.ino)
//     MQTT_PASS = ...                            (อันเดียวกับใน esp32_servo_controller.ino)
//     MQTT_PORT = 8883                           (ไม่ตั้งก็ใช้ 8883 เป็นค่าเริ่มต้นอยู่แล้ว)
//   แล้ว redeploy service นี้ 1 ครั้ง (npm install จะดึง "mqtt" ให้อัตโนมัติจาก package.json)

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt');

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

// ---------- MQTT bridge (ทำงานเฉพาะตอนตั้ง env vars ครบเท่านั้น) ----------
const TOPIC_CAM_STATUS = 'securitycam/cam/status';
let mqttBridge = null;
let camAudioReady = null; // null=ยังไม่เคยได้ยินจากกล้องเลย, '0' หรือ '1' ตามที่กล้องส่งมา
let camErrCode = null;

if (process.env.MQTT_HOST && process.env.MQTT_USER && process.env.MQTT_PASS) {
  // ตรวจพอร์ตให้เป็นตัวเลข 1-65535 ก่อนใช้งานจริง — ถ้าตั้ง MQTT_PORT ผิด (เช่น เผลอวางรหัสผ่าน/token
  // ใส่มาแทนพอร์ต) mqtt.connect() จะโยน RangeError ออกมา "ทันทีตอนเริ่มสคริปต์" (ไม่ใช่ event 'error'
  // ทีหลัง) ซึ่งเกิดก่อนที่ process.on('uncaughtException') ท้ายไฟล์จะถูกลงทะเบียนเสร็จด้วยซ้ำ —
  // ผลคือทั้ง service ล่มทันที กระทบวิดีโอ/เสียง/ควบคุมเซอร์โวไปด้วยทั้งที่ไม่เกี่ยวกัน
  // เช็ค+ห่อ try/catch ตรงนี้กันไว้ ต่อให้ตั้งค่าอะไรผิดพลาด service หลักจะไม่ล่มตามอีกต่อไป
  const mqttPortRaw = process.env.MQTT_PORT || '8883';
  const mqttPort = parseInt(mqttPortRaw, 10);
  if (!Number.isInteger(mqttPort) || mqttPort <= 0 || mqttPort > 65535) {
    console.log('[MQTT bridge] MQTT_PORT ค่าไม่ถูกต้อง: "' + mqttPortRaw + '" (ต้องเป็นตัวเลข 1-65535 ปกติคือ 8883) — เช็คว่าเผลอใส่ค่าอื่น (เช่นรหัสผ่าน) ผิดช่องหรือเปล่า ข้ามฟีเจอร์นี้ไปก่อน วิดีโอ/เสียง/ควบคุมยังทำงานปกติ');
  } else {
    try {
      mqttBridge = mqtt.connect('mqtts://' + process.env.MQTT_HOST + ':' + mqttPort, {
        username: process.env.MQTT_USER,
        password: process.env.MQTT_PASS,
        clientId: 'relay-bridge-' + Math.random().toString(16).slice(2),
        reconnectPeriod: 3000,
        connectTimeout: 8000,
      });
      mqttBridge.on('connect', () => console.log('[MQTT bridge] เชื่อมต่อ HiveMQ สำเร็จ'));
      mqttBridge.on('error', (e) => console.log('[MQTT bridge] error: ' + e.message));
    } catch (e) {
      console.log('[MQTT bridge] ตั้งค่าต่อ MQTT ไม่สำเร็จ: ' + e.message + ' — ข้ามฟีเจอร์นี้ไปก่อน วิดีโอ/เสียง/ควบคุมยังทำงานปกติ');
      mqttBridge = null;
    }
  }
} else {
  console.log('[MQTT bridge] ยังไม่ได้ตั้ง MQTT_HOST/MQTT_USER/MQTT_PASS — ข้ามฟีเจอร์นี้ (วิดีโอ/เสียง/ควบคุมยังใช้งานได้ปกติ)');
}

function publishCamStatus(offline) {
  if (!mqttBridge || !mqttBridge.connected) return;
  const payload = offline ? 'offline' : ('online;audio=' + (camAudioReady == null ? '?' : camAudioReady) + ';err=' + (camErrCode == null ? '?' : camErrCode));
  mqttBridge.publish(TOPIC_CAM_STATUS, payload, { retain: true });
}

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
          publishCamStatus(false);
        }
        return;
      }

      // ดักข้อความสถานะจากกล้อง (AUDIO_STATUS:/ERRCODE:) เพื่อ bridge เข้า MQTT เพิ่มเติม —
      // ไม่ได้แทนที่การส่งต่อให้ viewer.html ด้านล่าง แค่ "แอบอ่าน" เข้ามาด้วยเฉยๆ
      if (ws.isCamera && !isBinary) {
        const text = data.toString();
        if (text.startsWith('AUDIO_STATUS:')) { camAudioReady = text.split(':')[1]; publishCamStatus(false); }
        else if (text.startsWith('ERRCODE:'))  { camErrCode = text.split(':')[1];   publishCamStatus(false); }
      }

      if (ws.isCamera) {
        // ข้อมูลจากกล้อง (ภาพ/เสียง/ข้อความสถานะ) -> ส่งต่อให้ทุกมือถือที่ดูอยู่ (เหมือนเดิมทุกประการ)
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
    if (ws.isCamera) {
      camSocket = null;
      camAudioReady = null; camErrCode = null;
      console.log('กล้องตัดการเชื่อมต่อ');
      publishCamStatus(true);
    }
    else viewers.delete(ws);
  });

  if (!ws.isCamera) viewers.add(ws); // ผู้ชมทุกคนที่ยังไม่ auth เป็นกล้อง ถือว่าเป็น viewer ชั่วคราว
});

// กันเซิร์ฟเวอร์ทั้งตัวล่มจาก error ที่ไม่ได้ดักไว้จุดใดจุดหนึ่ง (Render จะไม่รีสตาร์ทให้ทันทีถ้า process ตายกลางอากาศ)
process.on('uncaughtException', (err) => console.log('uncaughtException: ' + err.message));
process.on('unhandledRejection', (err) => console.log('unhandledRejection: ' + err));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('relay server ทำงานที่พอร์ต ' + PORT));
