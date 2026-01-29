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

  // 5045 - Antigravity 裡的 opus (0 replies, 新串)
  // OP=1
  await insertReply(5045, '什麼是 Antigravity\n第一次聽到', '名無しさん', 3);
  await insertReply(5045, '>>1 好像是類似 cursor 的 AI 編輯器', '名無しさん', 2.5);
  await insertReply(5045, 'Claude Code 直接用 terminal\n比較習慣', '工程師', 2);
  await insertReply(5045, '每個平台的 system prompt 不一樣\n表現會有差', '名無しさん', 1.5);
  await insertReply(5045, 'Opus 4.5 本來就比較貴\n要看任務類型', '名無しさん', 1);
  console.log('✓ 5045 新增 5 則回覆');

  // 4998 - 處置股解禁潮 (5 replies)
  // OP=1, 4999=2, 5000=3, 5001=4, 5002=5, 5003=6
  await insertReply(4998, '>>3 矽光子跟 AI 算力綁在一起\n這波還沒完', '名無しさん', 8);
  await insertReply(4998, '出關前後價差大\n短線玩家愛這種', '名無しさん', 6);
  await insertReply(4998, '>>6 HBM 需求是真的猛\n但估值已經很高了', '名無しさん', 4);
  await insertReply(4998, '等跌深再買 不急', '名無しさん', 2);
  console.log('✓ 4998 新增 4 則回覆');

  // 5025 - 江蕙演唱會 (5 replies)
  // OP=1, 5026=2, 5027=3, 5028=4, 5029=5, 5030=6
  await insertReply(5025, '>>3 黃牛真的可惡\n演唱會票都被他們搞爛', '名無しさん', 14);
  await insertReply(5025, '我阿嬤超愛二姐\n可惜她走了 不然一定帶她去', '名無しさん', 12);
  await insertReply(5025, '實名制有用嗎\n感覺黃牛還是很多', '名無しさん', 10);
  await insertReply(5025, '>>8 有比沒有好\n至少沒那麼誇張', '名無しさん', 8);
  console.log('✓ 5025 新增 4 則回覆');

  // 4985 - DeepSeek AI (5 replies)
  // OP=1, 4986=2, 4987=3, 4988=4, 4989=5, 4990=6
  await insertReply(4985, '>>5 禁令效果有限\n中國本土化速度很快', '名無しさん', 14);
  await insertReply(4985, 'DeepSeek 開源這招很聰明\n直接搶生態系', '名無しさん', 12);
  await insertReply(4985, '>>6 研發人才台灣真的缺\n都被挖去美國', '名無しさん', 10);
  await insertReply(4985, 'AI 模型訓練成本太高\n只有大公司玩得起', '名無しさん', 8);
  await insertReply(4985, '>>8 所以小公司都用 API\n自己訓練不划算', 'AI工程師', 6);
  console.log('✓ 4985 新增 5 則回覆');

  // 5011 - CWT-72 (5 replies)
  // OP=1, 5012=2, 5013=3, 5014=4, 5015=5, 5016=6
  await insertReply(5011, '>>5 芙莉蓮+1\n感覺會有很多同人本', '名無しさん', 18);
  await insertReply(5011, '台中場 T35 也快了\n2/28', '名無しさん', 16);
  await insertReply(5011, '>>6 我每次都說只花3000\n結果回家數一數都破萬', '名無しさん', 14);
  await insertReply(5011, '排隊排到腳斷\n熱門攤太誇張', '名無しさん', 12);
  await insertReply(5011, '第一天去比較好\n東西比較齊全', '老場民', 10);
  console.log('✓ 5011 新增 5 則回覆');

  // 4965 - 育嬰留停新制 (5 replies)
  // OP=1, 4966=2, 4967=3, 4968=4, 4969=5, 4970=6
  await insertReply(4965, '>>3 臉色一定有\n但法律站在我們這邊', '名無しさん', 20);
  await insertReply(4965, '希望不只是大公司才敢用\n中小企業員工更需要', '名無しさん', 18);
  await insertReply(4965, '>>5 小時請假讚\n去接小孩剛剛好', '名無しさん', 16);
  await insertReply(4965, '生育率這麼低 政府終於醒了', '名無しさん', 14);
  console.log('✓ 4965 新增 4 則回覆');

  // 4663 - 圖片上傳 (5 replies)
  // OP=1, 4664=2, 4665=3, 4666=4, 4667=5, 4668=6
  await insertReply(4663, '>>4 imgur 外連+1\n最簡單的解法', '名無しさん', 20);
  await insertReply(4663, '圖片會讓討論變淺\n純文字逼你好好寫', '名無しさん', 18);
  await insertReply(4663, '>>6 同意 純文字有純文字的魅力', '名無しさん', 16);
  await insertReply(4663, '貼圖可能會有版權問題\n管理起來很麻煩', '名無しさん', 14);
  console.log('✓ 4663 新增 4 則回覆');

  // 4550 - 威力彩 (5 replies)
  // OP=1, 4551=2, 4552=3, 4553=4, 4554=5, 4555=6
  await insertReply(4550, '>>5 睡三天之後呢\n我要先辭職再睡三天', '名無しさん', 20);
  await insertReply(4550, '7.7億放定存\n每年利息都比我年薪高', '名無しさん', 18);
  await insertReply(4550, '機率這麼低不如把錢省下來\n但買個希望也好', '名無しさん', 16);
  await insertReply(4550, '中獎之後會不會一堆親戚冒出來', '名無しさん', 14);
  await insertReply(4550, '>>10 所以要匿名領獎\n誰都不能說', '名無しさん', 12);
  console.log('✓ 4550 新增 5 則回覆');

  // 4721 - Honnold 爬 101 直播 (5 replies)
  // OP=1, 4722=2, 4723=3, 4724=4, 4725=5, 4726=6
  await insertReply(4721, '91分鐘登頂\n我光看轉播手就在抖', '名無しさん', 22);
  await insertReply(4721, '>>5 哈哈 信義區那天好熱鬧', '名無しさん', 20);
  await insertReply(4721, 'Netflix 這行銷太厲害\n全世界都在討論台北', '名無しさん', 18);
  await insertReply(4721, '101 大樓方同意這個也很猛\n萬一出事怎麼辦', '名無しさん', 16);
  await insertReply(4721, '>>10 應該有保險吧\n這種極限運動一定要保', '名無しさん', 14);
  console.log('✓ 4721 新增 5 則回覆');

  // 4750 - ICE 射殺事件 (5 replies)
  // OP=1, 4751=2, 4752=3, 4753=4, 4754=5, 4755=6
  await insertReply(4750, '>>3 這個點中了\n媒體選擇性報導', '名無しさん', 22);
  await insertReply(4750, '美國警察暴力問題一直都有\n不是川普才開始', '名無しさん', 20);
  await insertReply(4750, '>>5 但川普讓這些單位更有恃無恐', '名無しさん', 18);
  await insertReply(4750, 'ICE 本來是抓非法移民的\n現在連公民都抓', '名無しさん', 16);
  console.log('✓ 4750 新增 4 則回覆');

  console.log('\n全部完成！');
}

main()
  .then(() => pool.end())
  .catch((err) => { console.error('錯誤:', err); pool.end(); process.exit(1); });
