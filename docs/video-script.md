# فیلم‌نامه‌ی ویدئو – تمرین ۱ سیستم‌های توزیع‌شده (متین قنبری)

**مدت کل: ۴:۵۸ (کمتر از ۵ دقیقه).** همه‌ی دستورها همان‌طورند که باید کپی و اجرا شوند. نشانی‌ها: سرور A = `192.168.75.10`، سرور B = `192.168.75.11`، ماشین کلاینت (و witness) = `192.168.75.20`. همه‌ی دستورها از پوشه‌ی `~/bank-assignment` اجرا می‌شوند.

## چیدمان صفحه

از ویندوز یک پنجره‌ی Windows Terminal با چهار پنل (۲×۲) باز کنید. در `cmd.exe` (نام کاربری `matin` را با نام کاربری واقعی خود عوض کنید):

```
wt ssh matin@192.168.75.10 ; sp -V ssh matin@192.168.75.11 ; mf left ; sp -H ssh matin@192.168.75.20 ; mf right ; sp -H ssh matin@192.168.75.20
```

| پنل | جای آن | ماشین | کاربرد |
|---|---|---|---|
| ۱ | بالا چپ | سرور A | داخل tmux: بالا = خروجی سرور، پایین (۸ خط) = شِل برای `kill`، `tail` و `vm_partition.sh` |
| ۲ | بالا راست | سرور B | خروجی سرور B |
| ۳ | پایین چپ | کلاینت | کلاینت و اسکریپت‌های demo |
| ۴ | پایین راست | کلاینت | witness در بخش امتیازی ۲ |

روی سرور A (پنل ۱) یک بار tmux را با دو بخش باز کنید:

```
tmux new -s A \; split-window -v -l 8 \; select-pane -t 0
```

جابه‌جایی میان دو بخش tmux: `Ctrl-b` و سپس کلید جهت بالا/پایین. در ادامه «A-بالا» یعنی بخش بالای پنل ۱ و «A-پایین» یعنی بخش پایین آن.

## جدول زمان‌بندی

| # | زمان | موضوع | الزام تمرین |
|---|---|---|---|
| ۱ | 00:00–00:10 | معرفی و سه VM | استقرار روی VMهای جدا |
| ۲ | 00:10–00:35 | عملیات عادی + لاگ | balance / deposit / withdraw، لاگ |
| ۳ | 00:35–00:55 | ۵۰ برداشت هم‌زمان | Isolation، چند کلاینت هم‌زمان |
| ۴ | 00:55–01:10 | ۸ کلاینت هم‌زمان | چند کلاینت هم‌زمان، Consistency |
| ۵ | 01:10–01:32 | همه‌ی حالت‌های خطا + لاگ | مدیریت خطا، لاگ |
| ۶ | 01:32–01:50 | خرابی وسط تراکنش | Atomicity |
| ۷ | 01:50–02:18 | `kill -9` و اجرای دوباره | Durability |
| ۸ | 02:18–02:35 | راه‌اندازی خوشه‌ی A/B | آماده‌سازی امتیازی ۱ |
| ۹ | 02:35–03:32 | سناریوی failover از ابتدا تا انتها | امتیازی ۱ |
| ۱۰ | 03:32–03:45 | راه‌اندازی witness و A/B | آماده‌سازی امتیازی ۲ |
| ۱۱ | 03:45–04:50 | سناریوی split brain از ابتدا تا انتها | امتیازی ۲ |
| ۱۲ | 04:50–04:58 | جمع‌بندی | – |

جمع: ۴ دقیقه و ۵۸ ثانیه. ریست‌های میان سناریوها (بخش‌های ۸ و ۱۰) را می‌توانید با Pause در OBS کوتاه کنید، ولی **درون یک سناریو هرگز Pause نکنید**: failover و split brain باید بی‌وقفه دیده شوند.

---

## بخش ۱ – 00:00–00:10 – معرفی

- **پنل‌ها:** هر چهار پنل.
- **دستور** (در هر پنل، پیش از ضبط آماده کنید):

```
hostname; hostname -I
```

- **آنچه باید دیده شود:** نام و IP هر سه ماشین (`192.168.75.10`، `192.168.75.11`، `192.168.75.20`).
- **گفتار:** «این سیستم بانکی روی سه ماشین مجازی جدا اجرا می‌شود: سرور A، سرور B و ماشین کلاینت که witness را هم اجرا می‌کند.»
- **یادداشت:** از قبل هر چهار پنل را در پوشه‌ی پروژه بگذارید و صفحه را `clear` کنید.

## بخش ۲ – 00:10–00:35 – عملیات عادی و لاگ

- **پنل‌ها:** A-بالا، پنل ۳، سپس A-پایین.
- **دستورها:**

```
# A-top (fresh bank.db)
python3 server.py
```

```
# client pane 3
python3 client.py --host 192.168.75.10 balance A1002
python3 client.py --host 192.168.75.10 deposit A1002 100
python3 client.py --host 192.168.75.10 withdraw A1002 30
```

```
# A-bottom
tail -n 3 logs/server.log
```

- **آنچه باید دیده شود:** پاسخ‌های JSON با `"status": "success"` و موجودی‌های 500.00 ← 600.00 ← 570.00؛ در لاگ سرور `client_ip` برابر `192.168.75.20` و فیلدهای `request_id`، `action`، `account`، `result`، `latency_ms`.
- **گفتار:** «کلاینت با IP واقعی سرور از طریق TCP و پیام‌های JSON تک‌خطی وصل می‌شود؛ هر درخواست با شناسه، IP کلاینت، نتیجه و تأخیر در لاگ ثبت می‌شود.»
- **یادداشت:** عملیات عادی را عمداً روی A1002 انجام دهید تا A1001 روی 1000 بماند و آزمون بخش ۳ دقیقاً 33/17/10.00 شود.

## بخش ۳ – 00:35–00:55 – پنجاه برداشت هم‌زمان (Isolation)

- **پنل‌ها:** پنل ۳ (و A-بالا که لاگ‌ها را سریع نشان می‌دهد).
- **دستور:**

```
python3 demo/concurrent_withdrawals.py --host 192.168.75.10 --clients 50 --amount 30
```

- **آنچه باید دیده شود:** `success 33`، `INSUFFICIENT_FUNDS 17`، `Final balance: 10.00 (expected 10.00)` و خط `PASS: no lost updates, no overdraft ...`.
- **گفتار:** «پنجاه کلاینت هم‌زمان از یک حساب سی واحد برداشت می‌کنند؛ BEGIN IMMEDIATE و به‌روزرسانی شرطی باعث می‌شوند دقیقاً ۳۳ برداشت موفق شوند و موجودی هرگز منفی نشود.»
- **یادداشت:** اگر A1001 در ابتدا 1000 نباشد، عددها فرق می‌کنند ولی آزمون باز هم PASS می‌شود؛ فقط گفتار را با عدد واقعی تطبیق دهید.

## بخش ۴ – 00:55–01:10 – هشت کلاینت هم‌زمان (Consistency)

- **پنل‌ها:** پنل ۳.
- **دستور:**

```
python3 demo/multi_clients.py --host 192.168.75.10 --clients 8 --ops 25
```

- **آنچه باید دیده شود:** خط‌های درهم‌ِ `client  0 … client  7`، سپس `requests: … succeeded, … rejected`، خط `total money before …, after …, expected …` و `PASS: money is conserved`.
- **گفتار:** «هشت کلاینت هم‌زمان واریز، برداشت، انتقال و پرس‌وجو انجام می‌دهند و مجموع پول دقیقاً برابر مقدار قبلی به‌علاوه‌ی واریزها منهای برداشت‌ها می‌ماند.»
- **یادداشت:** عددهای این اجرا با عددهای گزارش (189/11، 4577.63) فرق دارند چون وضعیت شروع متفاوت است؛ در گفتار عدد نخوانید.

## بخش ۵ – 01:10–01:32 – همه‌ی حالت‌های خطا و لاگ

- **پنل‌ها:** پنل ۳، سپس A-پایین.
- **دستورها:**

```
python3 demo/error_cases.py --host 192.168.75.10
```

```
# A-bottom
tail -n 4 logs/server.log
```

- **آنچه باید دیده شود:** ۱۷ خط `[PASS]` (حساب ناشناخته Z9999، کمبود موجودی، مبلغ‌های -50 / 0 / "abc" / true / NaN / Infinity / 10.005، عمل پشتیبانی‌نشده، فیلدهای جاافتاده، JSON خراب، بایت‌های تصادفی، خط 70 KB)، سپس `After all errors, same connection: balance A1001 -> …` و `ALL PASS`؛ در لاگ `result` برابر `MALFORMED_REQUEST` و غیره.
- **گفتار:** «هر ورودی نامعتبر یک کد خطای مشخص می‌گیرد، سرور از کار نمی‌افتد و حتی همان اتصال پس از بایت‌های تصادفی قابل استفاده می‌ماند.»
- **یادداشت:** چند ثانیه روی فهرست PASS مکث کنید تا داور هر شش حالت تمرین را ببیند.

## بخش ۶ – 01:32–01:50 – خرابی وسط تراکنش (Atomicity)

- **پنل‌ها:** A-بالا، پنل ۳.
- **دستورها:**

```
# A-top: Ctrl+C, then
BANK_ENABLE_FAULT_INJECTION=1 python3 server.py
```

```
# client pane 3
python3 demo/fault_injection_demo.py --host 192.168.75.10
```

- **آنچه باید دیده شود:** خط `FAULT INJECTION ENABLED - debug use only` در لاگ سرور؛ در کلاینت `before: …`، دو خط `INTERNAL_ERROR`، `after: …` با همان عددها و `PASS: failed transaction was rolled back completely`.
- **گفتار:** «سرور عمداً درست پس از برداشت و پیش از واریز خطا می‌دهد؛ تراکنش کامل برمی‌گردد و هر دو موجودی دست‌نخورده می‌مانند.»
- **یادداشت:** اگر `FAULT_INJECTION_DISABLED` دیدید، سرور بدون متغیر محیطی اجرا شده است؛ دوباره با همان دستور بالا اجرا کنید.

## بخش ۷ – 01:50–02:18 – kill -9 و اجرای دوباره (Durability)

- **پنل‌ها:** پنل ۳، A-پایین، A-بالا.
- **دستورها:**

```
# client pane 3 (makes 5 deposits, then waits for Enter)
python3 demo/durability_demo.py --host 192.168.75.10
```

```
# A-bottom
kill -9 $(pgrep -f server.py)
```

```
# A-top (the old server shows "Killed")
python3 server.py
```

سپس در پنل ۳ کلید Enter را بزنید.

- **آنچه باید دیده شود:** پنج خط `deposit #… success`، خط `… it must be X`، پیام `Killed` در A-بالا، اجرای دوباره‌ی سرور، و سپس `Balance after crash + restart: X` و `PASS: every acknowledged deposit survived the crash`.
- **گفتار:** «پنج واریز تأیید شد؛ حالا سرور را با kill -9 بدون هیچ فرصتی برای پاک‌سازی می‌کشیم و دوباره اجرا می‌کنیم؛ چون پاسخ فقط پس از COMMIT و fsync فرستاده می‌شود، همه‌ی واریزها باقی مانده‌اند.»
- **یادداشت:** با این دو بخش، هر چهار ویژگی ACID پوشش داده شد: Isolation (بخش ۳)، Consistency (بخش‌های ۴ و ۵)، Atomicity (بخش ۶) و Durability (بخش ۷).

## بخش ۸ – 02:18–02:35 – راه‌اندازی خوشه‌ی امتیازی ۱

- **پنل‌ها:** A-بالا، پنل ۲، پنل ۳.
- **دستورها:**

```
# A-top: Ctrl+C, then
rm -f bank.db bank.db-wal bank.db-shm
python3 server.py --node-id A --initial-role primary --peer 192.168.75.11:6000
```

```
# B (pane 2)
rm -f bank.db bank.db-wal bank.db-shm
python3 server.py --node-id B --initial-role backup  --peer 192.168.75.10:6000
```

```
# client pane 3
python3 client.py --host 192.168.75.11 balance A1001
```

- **آنچه باید دیده شود:** در B رویداد `snapshot_applied`، در A رویداد `snapshot_sent ... synchronous replication active`؛ در کلاینت پاسخ `NOT_PRIMARY` با `primary_hint`.
- **گفتار:** «حالا A سرور اصلی و B پشتیبان است؛ هر نوشتن پیش از پاسخ به کلاینت روی B commit می‌شود و B خودش به کلاینت‌ها پاسخ NOT_PRIMARY می‌دهد.»
- **یادداشت:** برای صرفه‌جویی در زمان می‌توانید هنگام `rm` و راه‌اندازی، OBS را Pause کنید و فقط لحظه‌ی `snapshot_applied` و `NOT_PRIMARY` را ضبط کنید.

## بخش ۹ – 02:35–03:32 – سناریوی failover (ابتدا تا انتها)

- **پنل‌ها:** پنل ۳ (بار کاری)، A-پایین (کشتن)، پنل ۲ (B)، A-بالا (بازگشت A).
- **دستورها:**

```
# client pane 3
python3 demo/failover_demo.py --servers 192.168.75.10:5000,192.168.75.11:5000 --duration 30
```

```
# A-bottom, at about 10 s (while the clients are sending)
kill -9 $(pgrep -f "server.py --node-id A")
```

```
# A-top, at about 20 s (A rejoins as backup)
python3 server.py --node-id A --initial-role primary --peer 192.168.75.11:6000
```

- **آنچه باید دیده شود:** خط‌های پیشرفت هر ۲ ثانیه با `by server:`؛ پیام‌های `retrying same request on next server` و `answered by 192.168.75.11:5000 after … attempts` در کلاینت؛ در B رویدادهای `primary_failure_detected` و `promoted` با `epoch 2` حدود ۳ ثانیه بعد؛ پاسخ‌دهی B در خط‌های پیشرفت؛ در A-بالا `role_change` به `new_role backup` و `snapshot_applied`؛ در پایان بخش `RESULT` با موجودی initial / expected / final برای هر سه حساب و چهار خط `[PASS]` و `PASS`.
- **گفتار:** «شش کلاینت در حال ارسال‌اند و سرور A را با kill -9 می‌کشیم. پس از حدود سه ثانیه بی‌خبری، B با epoch دو سرور اصلی می‌شود و کلاینت‌ها همان درخواست با همان request_id را دوباره به B می‌فرستند؛ در پایان موجودی‌ها دقیقاً برابر مقدار مورد انتظارند و هیچ تراکنشی گم یا دوبار اعمال نشده است. A هم پس از بازگشت، پشتیبان می‌شود.»
- **یادداشت:** README مدت 40 ثانیه را پیشنهاد می‌کند (عددهای گزارش از اجرای 40 ثانیه‌ای است)؛ برای جا شدن در ۵ دقیقه 30 ثانیه کافی است. مرحله‌ی بررسی پایانی چند ثانیه طول می‌کشد چون همه‌ی درخواست‌ها را دوباره می‌فرستد؛ صبر کنید و قطع نکنید.

## بخش ۱۰ – 03:32–03:45 – راه‌اندازی witness و خوشه‌ی امتیازی ۲

- **پنل‌ها:** پنل ۴، A-بالا، پنل ۲، پنل ۳.
- **دستورها:**

```
# A-top and B: Ctrl+C, then on both
rm -f bank.db bank.db-wal bank.db-shm
```

```
# client pane 4 (witness first)
rm -f witness_state.json
python3 witness.py --port 7000
```

```
# A-top
python3 server.py --node-id A --initial-role primary --peer 192.168.75.11:6000 --witness 192.168.75.20:7000
```

```
# B (pane 2)
python3 server.py --node-id B --initial-role backup --peer 192.168.75.10:6000 --witness 192.168.75.20:7000
```

```
# client pane 3
python3 witness.py --status 192.168.75.20:7000
```

- **آنچه باید دیده شود:** در A رویداد `lease_granted` با epoch 1، در B `snapshot_applied`، در witness رویدادهای `lease_granted` و `sync_report` با `in_sync true`؛ خروجی status با `"holder": "A"` و `"in_sync": true`.
- **گفتار:** «برای جلوگیری از split brain یک witness روی ماشین کلاینت رأی سوم است؛ A فقط وقتی سرویس می‌دهد که lease معتبر از witness داشته باشد.»
- **یادداشت:** این بخش را هم می‌توانید با Pause کوتاه کنید؛ فقط حتماً خروجی `--status` با `in_sync: true` را پیش از شروع بخش ۱۱ ضبط کنید.

## بخش ۱۱ – 03:45–04:50 – سناریوی split brain (ابتدا تا انتها)

- **پنل‌ها:** پنل ۳ (demo و Enter)، A-پایین (iptables)، پنل ۲ (B)، A-بالا (A)، پنل ۴ (witness).
- **دستور شروع (پنل ۳):**

```
python3 demo/split_brain_demo.py --servers 192.168.75.10:5000,192.168.75.11:5000 \
    --repl 192.168.75.10:6000,192.168.75.11:6000 --witness 127.0.0.1:7000
```

- **ترتیب کار** (پس از هر دستور در A-پایین، در پنل ۳ کلید Enter را بزنید):

```
# A-bottom, step 1 (~3 s after start): A<->B cut, both still reach the witness; wait ~8 s
sudo bash demo/vm_partition.sh cut-link
```

```
# A-bottom, step 2: heal; wait ~5 s
sudo bash demo/vm_partition.sh heal
```

```
# A-bottom, step 3: A cut from B AND from witness port 7000; wait ~8 s
sudo bash demo/vm_partition.sh cut-all
```

```
# A-bottom, step 4: heal; wait ~6 s, then confirm no rules are left
sudo bash demo/vm_partition.sh heal
sudo bash demo/vm_partition.sh show
```

- **آنچه باید دیده شود:**
  - پس از `cut-link`: در A رویداد `degraded_mode`، در B رویداد `promotion_refused` (lease held by A)، در خروجی demo مقدار `"holder": "A"`؛ A همچنان پاسخ می‌دهد.
  - پس از `heal`: در A رویداد `snapshot_sent` و در witness `sync_report` با `in_sync true`.
  - پس از `cut-all`: در A رویداد `lease_lost ... FENCED` و پاسخ‌های `NO_QUORUM`؛ چند ثانیه بعد در B رویداد `promoted` با `epoch 2` و در witness `lease_expired` و `lease_granted` برای B.
  - پس از `heal` دوم: در A رویداد `role_change` با `new_role backup` و `snapshot_applied`؛ خروجی `show` برابر `no partition rules`.
  - در پایان: `acks per epoch` (epoch 1 فقط node A، epoch 2 فقط node B)، دو خط `status` با `digest` یکسان، پنج خط `[PASS]` و `OVERALL PASS`.
- **گفتار:** «اول فقط پیوند A و B را قطع می‌کنیم: A هنوز lease دارد و به‌تنهایی سرویس می‌دهد و witness ارتقای B را رد می‌کند. بعد A را از B و witness هم جدا می‌کنیم: A پیش از پایان lease خودش را متوقف می‌کند و NO_QUORUM می‌دهد، و فقط پس از آن B با epoch دو سرور اصلی می‌شود؛ پس هیچ لحظه‌ای دو سرور اصلی نداریم. پس از وصل دوباره، A کنار می‌رود، داده‌ی B را می‌گیرد و هر دو digest یکسان دارند.»
- **یادداشت:** پیش از ضبط یک بار `sudo -v` را روی A بزنید تا sudo وسط ضبط رمز نخواهد. پس از `heal` دوم ممکن است چند ثانیه طول بکشد تا اتصال‌های TCP آویزان timeout شوند؛ تا دیدن `role_change` صبر کنید و بعد Enter بزنید.

## بخش ۱۲ – 04:50–04:58 – جمع‌بندی

- **پنل‌ها:** هر چهار پنل (خروجی‌های PASS روی صفحه).
- **دستور:** ندارد.
- **آنچه باید دیده شود:** `OVERALL PASS` در پنل ۳.
- **گفتار:** «همه‌ی آزمون‌ها روی سه ماشین مجازی جدا با موفقیت انجام شد؛ جزئیات طراحی و نتایج در گزارش آمده است.»
- **یادداشت:** کنترل منفی `--without-witness` را در ویدئو اجرا نکنید؛ فقط در گزارش (به‌عنوان اجرای محلی) آمده است.

---

## چک‌لیست پیش از ضبط

1. پروژه روی هر سه VM با یک نسخه (کامیت `9a79f27`) کپی شده باشد و `python3 --version` نسخه‌ی 3.12 را نشان دهد.
2. اگر ufw فعال است: روی A و B دستور `sudo ufw allow 5000/tcp; sudo ufw allow 6000/tcp` و روی کلاینت `sudo ufw allow 7000/tcp`.
3. هیچ سرور یا witness قدیمی در حال اجرا نباشد (روی هر سه VM):

```
pkill -f server.py; pkill -f witness.py; ss -ltnp | grep -E ':5000|:6000|:7000'
```

4. ریست پایگاه‌داده روی A و B (و در صورت تمایل پاک کردن لاگ‌ها برای خروجی تمیز):

```
rm -f bank.db bank.db-wal bank.db-shm
rm -f logs/server.log
```

5. ریست witness روی ماشین کلاینت:

```
rm -f witness_state.json logs/witness.log
```

6. بررسی پاک بودن قواعد iptables روی A (باید `no partition rules` چاپ شود):

```
sudo bash demo/vm_partition.sh heal
sudo bash demo/vm_partition.sh show
```

7. درست پیش از ضبط روی A دستور `sudo -v` را بزنید.
8. بررسی شبکه از کلاینت: `ping -c1 192.168.75.10` و `ping -c1 192.168.75.11`.
9. ترتیب راه‌اندازی: بخش اصلی فقط `python3 server.py` روی A؛ امتیازی ۱: A و B (ترتیب مهم نیست، هر دو با پایگاه‌داده‌ی تازه)؛ امتیازی ۲: اول witness، بعد A، بعد B.
10. tmux روی A با دو بخش آماده باشد (دستور در بخش «چیدمان صفحه»).
11. اندازه‌ی فونت Windows Terminal را روی 14 تا 16 (مثلاً Cascadia Mono) بگذارید، پنجره را تمام‌صفحه کنید و رزولوشن 1920×1080 باشد؛ پیش از هر بخش `clear` بزنید.
12. ضبط با OBS Studio روی ویندوز: منبع Display Capture یا Window Capture، خروجی 1080p و 30fps، قالب MKV (پس از ضبط از منوی File ← Remux به MP4 تبدیل کنید)؛ برای Start/Pause کلید میان‌بر تعریف کنید؛ اعلان‌های ویندوز را خاموش کنید (Focus Assist).
13. همه‌ی دستورها را در یک فایل Notepad آماده‌ی کپی داشته باشید؛ یک بار کل سناریو را بدون ضبط تمرین کنید و زمان بگیرید.

## برنامه‌ی پشتیبان (اگر مرحله‌ای جلوی دوربین خراب شد)

- **قاعده‌ی کلی:** OBS را Pause کنید، با `Ctrl+C` متوقف کنید، ریست کنید (چک‌لیست ۳ تا ۶) و همان بخش را از اول اجرا کنید. سناریوهای failover و split brain را همیشه از اول و بی‌وقفه ضبط کنید.
- **عددهای بخش ۳ 33/17/10.00 نشد:** پایگاه‌داده تازه نبوده است. آزمون همچنان PASS است؛ گفتار را با عدد واقعی بگویید یا A را ریست کنید و بخش‌های ۲ و ۳ را دوباره بگیرید.
- **`FAULT_INJECTION_DISABLED` در بخش ۶:** سرور را با `BANK_ENABLE_FAULT_INJECTION=1 python3 server.py` دوباره اجرا کنید.
- **در بخش ۹، B ارتقا نیافت:** در لاگ B به‌دنبال `cannot_promote` بگردید (یعنی B هرگز snapshot نگرفته است). هر دو سرور را متوقف و `rm -f bank.db*` کنید، اول A و بعد B را اجرا کنید و منتظر `snapshot_applied` بمانید.
- **کلاینت در بخش ۹ مدت زیادی معطل ماند:** طبیعی است که تا حدود ۴ ثانیه پاسخی نیاید؛ اگر بیشتر شد، بررسی کنید فقط فرایند A کشته شده باشد (`pgrep -af server.py` روی B).
- **در بخش ۱۱، B پس از `cut-all` ارتقا نیافت:** با `python3 witness.py --status 192.168.75.20:7000` وضعیت را ببینید و چند ثانیه بیشتر صبر کنید (انقضای lease + grace + مهلت 3 ثانیه‌ای). اگر `in_sync` برابر false است، پس از `cut-link` قبلی هنوز re-sync انجام نشده بود؛ پس از `heal` اول تا دیدن `snapshot_sent` صبر کنید.
- **A پس از `heal` دوم پشتیبان نشد:** چند ثانیه صبر کنید تا اتصال‌های قدیمی timeout شوند و `sudo bash demo/vm_partition.sh show` را بزنید؛ اگر قاعده‌ای مانده، `heal` را تکرار کنید.
- **SSH قطع شد:** `cut-all` فقط ترافیک B و درگاه 7000 را می‌بندد و SSH از ویندوز را قطع نمی‌کند؛ اگر به A از طریق ماشین B وصل شده‌اید، مستقیم از ویندوز وصل شوید.
- **آخرین راه:** یک ضبط کامل و موفق قبلی را نگه دارید. اجرای `--local` روی یک ماشین فقط برای توضیح است و جای اجرای روی سه VM را نمی‌گیرد، چون تمرین قطع واقعی شبکه میان VMها را می‌خواهد.
