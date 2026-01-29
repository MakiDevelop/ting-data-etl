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

async function getBoardId(slug: string): Promise<number | null> {
  const result = await pool.query('SELECT id FROM boards WHERE slug = $1', [slug]);
  return result.rows[0]?.id || null;
}

async function insertThread(
  boardSlug: string,
  title: string,
  content: string,
  authorName: string = '名無しさん',
  hoursAgo: number = 24
): Promise<number> {
  const boardId = await getBoardId(boardSlug);
  if (!boardId) throw new Error(`Board not found: ${boardSlug}`);
  const result = await pool.query(
    `INSERT INTO posts (title, content, status, ip_hash, user_agent, board_id, author_name, created_at)
     VALUES ($1, $2, 0, $3, $4, $5, $6, NOW() - INTERVAL '1 hour' * $7)
     RETURNING id`,
    [title, content, generateIpHash(), randomUserAgent(), boardId, authorName, hoursAgo]
  );
  return result.rows[0].id;
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
  console.log('開始新增討論串...\n');

  // ===== work 版 - 職場新制 =====
  let threadId = await insertThread(
    'work',
    '育嬰留停可以「按日請」了 這個新制超讚',
    '2026新制上路\n育嬰留停從本來最少要請30天\n改成可以按「日」為單位申請\n\n父母合計有60天彈性額度\n而且6個月內還是有8成薪補貼\n\n終於不用為了接小孩放學請一整個月了\n這個對雙薪家庭超級實用',
    '新手爸爸',
    36
  );
  await insertReply(threadId, '這個讚 之前育嬰假要請就要請整個月\n很多人根本不敢請', '名無しさん', 34);
  await insertReply(threadId, '8成薪還不錯 但公司會不會給臉色', '名無しさん', 32);
  await insertReply(threadId, '>>2 法律保障 公司敢刁難直接檢舉', '名無しさん', 30);
  await insertReply(threadId, '家庭照顧假也可以按小時請了\n這個更實用', '名無しさん', 28);
  await insertReply(threadId, '台灣終於跟上其他國家了\n希望不是紙上德政', '名無しさん', 25);
  console.log('✓ work 版：育嬰留停新制');

  // ===== work 版 - 最低工資 =====
  threadId = await insertThread(
    'work',
    '基本工資要破三萬了 十年漲47%',
    '2026年最低工資調到29500\n明年搞不好就破三萬了\n\n從2016年的20008到現在\n十年漲了47.4%\n時薪從120漲到196\n\n但物價漲多少大家心裡有數\n房價更不用說了',
    '名無しさん',
    30
  );
  await insertReply(threadId, '漲基本工資 = 漲物價\n無限循環', '名無しさん', 28);
  await insertReply(threadId, '>>1 至少有漲 總比不漲好', '名無しさん', 26);
  await insertReply(threadId, '時薪196還是很低\n便利商店店員真的辛苦', '打工仔', 24);
  await insertReply(threadId, '重點是很多老闆根本不遵守\n勞檢要加強', '名無しさん', 22);
  await insertReply(threadId, '>>4 這個+1 尤其餐飲業', '名無しさん', 20);
  await insertReply(threadId, '房價漲300%\n薪資漲47%\n笑死', '躺平族', 18);
  console.log('✓ work 版：基本工資調漲');

  // ===== tech 版 - Google Chrome AI Agent =====
  threadId = await insertThread(
    'tech',
    'Chrome 推出 AI Agent 可以幫你瀏覽網頁了',
    'Google 宣布 Chrome 的 AI Agent 功能\n可以代替使用者瀏覽網頁\n\n就是你告訴它要幹嘛\n它會自己開網頁、點按鈕、填表單\n\n感覺以後連上網都不用自己來了\n不知道是方便還是恐怖',
    'AI觀察者',
    24
  );
  await insertReply(threadId, '這不就是 RPA 的進化版嗎\n自動化搶票不是夢', '名無しさん', 22);
  await insertReply(threadId, '>>1 搶票仔狂喜', '名無しさん', 20);
  await insertReply(threadId, '隱私問題怎麼處理\nAI 幫你瀏覽 = Google 看光光', '資安人', 18);
  await insertReply(threadId, '以後工作是不是叫 AI 幫忙上班就好', '名無しさん', 16);
  await insertReply(threadId, '>>4 老闆：那我直接請 AI 就好\n你可以走了', '名無しさん', 14);
  await insertReply(threadId, 'Claude Code 已經可以幫寫程式了\n現在連上網都外包給 AI', '工程師', 12);
  console.log('✓ tech 版：Chrome AI Agent');

  // ===== tech 版 - DeepSeek =====
  threadId = await insertThread(
    'tech',
    'DeepSeek 讓矽谷巨頭都在怕 中國 AI 要追上來了？',
    '最近看到新聞說\nGoogle 和 Anthropic 的執行長\n在討論 DeepSeek 引發的矽谷恐懼\n\n中國 AI 技術進步很快\n尤其在推理能力方面\n\n台灣夾在中間\n半導體優勢能撐多久',
    '名無しさん',
    28
  );
  await insertReply(threadId, '中國 AI 進步是真的\n但頂尖人才還是往美國跑', '名無しさん', 26);
  await insertReply(threadId, '台積電還是關鍵\n誰有先進製程誰就贏', '半導體人', 24);
  await insertReply(threadId, '>>2 但中國也在狂蓋晶圓廠', '名無しさん', 22);
  await insertReply(threadId, 'AI 算力太吃晶片了\n美國禁令有用嗎', '名無しさん', 20);
  await insertReply(threadId, '台灣要站穩腳步\n不能只靠製造 研發也要跟上', '名無しさん', 18);
  console.log('✓ tech 版：DeepSeek 與 AI 競爭');

  // ===== money 版 - 台股創新高 =====
  threadId = await insertThread(
    'money',
    '台股衝上32803點 外資狂買332億',
    '今天大漲485點\n收在32803點 又是歷史新高\n\n外資買超332億\n台積電盤後鉅額交易創天價1838.97元\n\n三萬點說是地板\n現在看來還真的是',
    '多軍',
    12
  );
  await insertReply(threadId, '沒買台積電的都在哭', '名無しさん', 10);
  await insertReply(threadId, '>>1 我200塊的時候就賣了\n不想說了', '名無しさん', 9);
  await insertReply(threadId, '里昂喊台積電3000元\n瘋了吧', '名無しさん', 8);
  await insertReply(threadId, '這波是 AI 帶動的\n沒有 AI 題材的股票都躺平', '名無しさん', 7);
  await insertReply(threadId, '>>2 我100塊賣的 更不想說', '韭菜', 6);
  await insertReply(threadId, '外資一直買 散戶一直賣\n結果就是這樣', '名無しさん', 5);
  console.log('✓ money 版：台股創新高');

  // ===== money 版 - 處置股解禁 =====
  threadId = await insertThread(
    'money',
    '2026處置股解禁潮 15檔1月陸續出關',
    '過年前處置股大解禁\n超過15檔這個月會解除交易限制\n\n主要是記憶體封測、光電\n矽光子、電子零組件\n\n有人要接刀嗎',
    '名無しさん',
    20
  );
  await insertReply(threadId, '處置股出關通常先跌一波\n觀望', '名無しさん', 18);
  await insertReply(threadId, '矽光子題材還沒完\n可以注意', '名無しさん', 16);
  await insertReply(threadId, '>>1 也有出關就噴的\n看基本面', '名無しさん', 14);
  await insertReply(threadId, '處置期間籌碼洗乾淨了\n主力要拉就拉', '技術派', 12);
  await insertReply(threadId, '記憶體股要注意\nHBM 需求還是很強', '名無しさん', 10);
  console.log('✓ money 版：處置股解禁');

  // ===== acg 版 - 買動漫開幕 =====
  threadId = await insertThread(
    'acg',
    '買動漫台北車站旗艦店開幕了 有人去過嗎',
    '1/2 開幕的\n在台北車站 M6 出口附近\n新光三越館前店後面\n\n有書店區、展覽區、咖啡區\n第一場展覽是桂 Gui 老師的個展\n\n感覺可以去朝聖一下',
    'ACG宅',
    40
  );
  await insertReply(threadId, '去過了 場地比想像中大\n咖啡還行', '名無しさん', 38);
  await insertReply(threadId, '書的價格怎麼樣\n有比網購便宜嗎', '名無しさん', 36);
  await insertReply(threadId, '>>2 差不多 但可以現場翻\n確定喜歡再買', '名無しさん', 34);
  await insertReply(threadId, '假日人很多要有心理準備', '名無しさん', 32);
  await insertReply(threadId, '台北車站交通方便\n下班可以順路去', '名無しさん', 30);
  await insertReply(threadId, '希望能撐久一點\n實體店越來越少了', '名無しさん', 28);
  console.log('✓ acg 版：買動漫開幕');

  // ===== acg 版 - CWT =====
  threadId = await insertThread(
    'acg',
    'CWT-72 台北場 2月要來了',
    '2/21-22 在台大體育館\n距離上次好像也沒多久\n\n有人要去嗎\n今年有什麼熱門作品的攤',
    '名無しさん',
    32
  );
  await insertReply(threadId, '咒術完結後熱度還在嗎', '名無しさん', 30);
  await insertReply(threadId, '藍色監獄應該很多', '名無しさん', 28);
  await insertReply(threadId, '>>1 咒術還是有 但沒以前誇張', '名無しさん', 26);
  await insertReply(threadId, '葬送的芙莉蓮應該會很熱門', '名無しさん', 24);
  await insertReply(threadId, '每次去都花一堆錢\n荷包在哭', '名無しさん', 22);
  console.log('✓ acg 版：CWT-72');

  // ===== news 版 - 新台幣改版 =====
  threadId = await insertThread(
    'news',
    '新台幣24年來首次改版 網站開放投票101和晶片超熱門',
    '央行宣布新台幣要改版了\n睽違24年\n\n新版以「台灣之美」為主題\n開放民眾票選12項面額主題\n\n27號開放投票第一天就破萬人\n101、半導體晶片、黑熊最熱門\n系統還當機',
    '名無しさん',
    18
  );
  await insertReply(threadId, '終於要改了\n現在的設計真的很舊', '名無しさん', 16);
  await insertReply(threadId, '希望不要有政治人物頭像\n拜託', '名無しさん', 14);
  await insertReply(threadId, '>>2 同意 放風景或動物最好', '名無しさん', 12);
  await insertReply(threadId, '晶片放上去很有台灣特色\n半導體之島', '名無しさん', 10);
  await insertReply(threadId, '黑熊讚 可愛又有代表性', '名無しさん', 8);
  await insertReply(threadId, '現在都用行動支付\n誰還在用現金', '名無しさん', 6);
  await insertReply(threadId, '>>6 還是很多人用好嗎\n夜市攤販', '名無しさん', 4);
  console.log('✓ news 版：新台幣改版');

  // ===== gossip 版 - 江蕙安可場 =====
  threadId = await insertThread(
    'gossip',
    '江蕙演唱會安可場2月開唱 這次搶到票了嗎',
    '「無,有」安可場\n2/20-25 台北小巨蛋\n\n上次開賣秒殺\n這次不知道還有沒有機會\n\n二姐的歌真的經典\n想帶爸媽去聽',
    '名無しさん',
    26
  );
  await insertReply(threadId, '我媽超想去\n結果沒搶到', '名無しさん', 24);
  await insertReply(threadId, '黃牛價嚇死人\n原價3倍起跳', '名無しさん', 22);
  await insertReply(threadId, '>>2 江蕙的票黃牛都很兇', '名無しさん', 20);
  await insertReply(threadId, '二姐唱功無話說\n台語歌后', '名無しさん', 18);
  await insertReply(threadId, '希望之後還有加場\n一定要搶', '名無しさん', 16);
  console.log('✓ gossip 版：江蕙演唱會');

  // ===== love 版 - 約會趨勢 =====
  threadId = await insertThread(
    'love',
    '交友軟體報告說2026流行直球戀愛 是真的嗎',
    '看到 Tinder 報告說\n2026年趨勢是「直球戀愛」\n就是不玩曖昧 直接表態\n\n還有「慢節奏約會」\n先當朋友再說\n\n大家覺得準嗎',
    '單身狗',
    34
  );
  await insertReply(threadId, '每年都說不一樣\n誰信啊', '名無しさん', 32);
  await insertReply(threadId, '直球是好事\n曖昧太累了', '名無しさん', 30);
  await insertReply(threadId, '>>2 但一開始太直接會嚇到人', '名無しさん', 28);
  await insertReply(threadId, '慢節奏約會+1\n現代人太急了', '名無しさん', 26);
  await insertReply(threadId, '交友軟體本來就是速食文化\n慢什麼', '名無しさん', 24);
  await insertReply(threadId, '>>5 所以才要改變啊\n配對率太低', '名無しさん', 22);
  console.log('✓ love 版：約會趨勢');

  // ===== life 版 - 小年夜放假 =====
  threadId = await insertThread(
    'life',
    '2026起小年夜正式列入國定假日',
    '終於\n小年夜變成國定假日了\n\n以前除夕才放\n現在提前一天\n\n而且國定假日遇到休息日可以補假\n不會被吃掉',
    '社畜',
    38
  );
  await insertReply(threadId, '德政！可以提早返鄉了', '名無しさん', 36);
  await insertReply(threadId, '服務業表示：跟我無關', '名無しさん', 34);
  await insertReply(threadId, '>>2 服務業有加班費啊', '名無しさん', 32);
  await insertReply(threadId, '>>3 有給足額的算佛心老闆', '名無しさん', 30);
  await insertReply(threadId, '高鐵票更難搶了', '名無しさん', 28);
  await insertReply(threadId, '>>5 這個真的 提早一天出發的人變多', '名無しさん', 26);
  console.log('✓ life 版：小年夜放假');

  console.log('\n全部完成！共新增 12 個討論串');
}

main()
  .then(() => pool.end())
  .catch((err) => { console.error('錯誤:', err); pool.end(); process.exit(1); });
