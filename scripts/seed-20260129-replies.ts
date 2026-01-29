#!/usr/bin/env tsx
import { Pool } from 'pg';
import crypto from 'crypto';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/2ch',
});

function generateIpHash(): string {
  const randomIp = `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`;
  return crypto.createHash('sha256').update(randomIp).digest('hex');
}

const userAgents = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
  'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36',
];

function randomUserAgent(): string {
  return userAgents[Math.floor(Math.random() * userAgents.length)];
}

async function insertReply(
  parentId: number,
  content: string,
  authorName: string = '名無しさん',
  hoursAgo: number = 1
): Promise<void> {
  await pool.query(
    `INSERT INTO posts (content, status, ip_hash, user_agent, parent_id, board_id, author_name, created_at)
     VALUES ($1, 0, $2, $3, $4, NULL, $5, NOW() - INTERVAL '1 hour' * $6)`,
    [content, generateIpHash(), randomUserAgent(), parentId, authorName, hoursAgo]
  );
}

async function main() {
  console.log('開始新增回覆...');

  // 4941 - 討論版列表能固定嗎? (0 replies)
  // OP=1
  await insertReply(4941, '同意 版塊多的時候每次都要滑很久', '名無しさん', 2);
  await insertReply(4941, '可以用書籤功能\n把常用的版存起來', '名無しさん', 1.5);
  await insertReply(4941, '>>2 有書籤功能？在哪', '名無しさん', 1);
  await insertReply(4941, '希望可以自訂順序\n像PTT那樣把最愛的版排前面', '名無しさん', 0.5);
  console.log('✓ 4941 新增 4 則回覆');

  // 4611 - 手機版滑動有時候會卡卡的 (4 replies)
  // OP=1, 4612=2, 4613=3, 4614=4, 4615=5
  await insertReply(4611, '>>3 lazy loading +1\n現代網頁基本功能', '名無しさん', 12);
  await insertReply(4611, '我用Safari也會\n圖片loading的時候整個頁面會閃', '名無しさん', 10);
  await insertReply(4611, '說到手機版 能不能加個深色模式\n半夜滑眼睛很痛', '夜貓子', 8);
  await insertReply(4611, '>>8 深色模式+1\n超需要', '名無しさん', 6);
  console.log('✓ 4611 新增 4 則回覆');

  // 4738 - 矽谷搶能源專才 AI跟國防帶動新一波挖角 (5 replies)
  // OP=1, 4739=2, 4740=3, 4741=4, 4742=5, 4743=6
  await insertReply(4738, '>>5 台電員工薪水那麼低\n被挖走也是正常的', '名無しさん', 20);
  await insertReply(4738, '不只電力 冷卻系統專家也很缺\n資料中心散熱是大問題', '名無しさん', 18);
  await insertReply(4738, '>>6 核能重啟是遲早的事\n綠能根本不夠用', '名無しさん', 15);
  await insertReply(4738, '微軟都在搞核融合了\n台灣還在吵要不要重啟核四', '名無しさん', 12);
  await insertReply(4738, '>>4 AI改變生產力是真的\n但現在估值太高也是真的', '理性派', 10);
  console.log('✓ 4738 新增 5 則回覆');

  // 4581 - 2026冬季新番55部 你追什麼 (5 replies)
  // OP=1, 4582=2, 4583=3, 4584=4, 4585=5, 4586=6
  await insertReply(4581, '葬送的芙莉蓮第二季必追\n第一季神作', '名無しさん', 22);
  await insertReply(4581, '>>7 芙莉蓮+1\n但要等到幾月才播', '名無しさん', 20);
  await insertReply(4581, '我都等完結再一次看完\n追番太累了', '名無しさん', 18);
  await insertReply(4581, '>>4 異世界轉生也是\n套路都一樣', '名無しさん', 15);
  await insertReply(4581, '推薦一個冷門的\n「物語」系列新作值得追', '老宅', 12);
  console.log('✓ 4581 新增 5 則回覆');

  // 4575 - 考駕照要取消是非題了？還有機車路考 (5 replies)
  // OP=1, 4576=2, 4577=3, 4578=4, 4579=5, 4580=6
  await insertReply(4575, '日本機車分級超嚴格\n台灣什麼車都能騎才可怕', '名無しさん', 24);
  await insertReply(4575, '>>6 重機騎士表示同意\n一堆人連煞車都不會', '重機仔', 22);
  await insertReply(4575, '路考的話監理站場地夠嗎\n還是要去外面道路考', '名無しさん', 18);
  await insertReply(4575, '>>4 台灣道路設計真的爛\n機車道跟停車格混在一起', '名無しさん', 14);
  await insertReply(4575, '支持加難度\n現在太多拿到駕照還不會騎的', '名無しさん', 10);
  console.log('✓ 4575 新增 5 則回覆');

  console.log('\n全部完成！');
}

main()
  .then(() => pool.end())
  .catch((err) => { console.error('錯誤:', err); pool.end(); process.exit(1); });
