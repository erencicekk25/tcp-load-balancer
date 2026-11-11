# tcp-load-balancer

[![CI](https://github.com/erencicekk25/tcp-load-balancer/actions/workflows/ci.yml/badge.svg)](https://github.com/erencicekk25/tcp-load-balancer/actions/workflows/ci.yml)
![C++17](https://img.shields.io/badge/C%2B%2B-17-00599C?logo=cplusplus&logoColor=white)
![CMake](https://img.shields.io/badge/build-CMake-064F8C?logo=cmake&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

Modern C++17 ile sıfırdan yazılmış, **Round-Robin** algoritmasıyla çalışan, tek iş parçacıklı ve **non-blocking** bir TCP yük dengeleyici.

Bu projeyi, Nginx ya da HAProxy gibi araçların perde arkasında aslında ne yaptığını gerçekten anlamak için yazdım. Hazır bir framework ya da harici kütüphane yok; sadece işletim sisteminin soket API'si, `poll()` ve biraz da sabır var. Sonuç olarak ortaya hem Windows'ta (Winsock) hem de Linux/macOS'ta (POSIX) derlenen, küçük ama gerçek anlamda çalışan bir L4 yük dengeleyici çıktı.

---

## İçindekiler

- [Öne Çıkanlar](#öne-çıkanlar)
- [Hızlı Başlangıç](#hızlı-başlangıç)
- [Yük Dengeleme Nedir?](#yük-dengeleme-nedir)
- [Yük Dengeleme Algoritmaları](#yük-dengeleme-algoritmaları)
- [Mimari: Trafik Nasıl Yönlendiriliyor?](#mimari-trafik-nasıl-yönlendiriliyor)
- [Proje Yapısı](#proje-yapısı)
- [Yapılandırma](#yapılandırma)
- [Bilinen Sınırlamalar ve Yol Haritası](#bilinen-sınırlamalar-ve-yol-haritası)

---

## Öne Çıkanlar

- **Round-Robin dağıtım:** Gelen her bağlantı sıradaki sunucuya gider; yük eşit şekilde paylaşılır.
- **Non-blocking soket motoru:** Tek bir olay döngüsü (`poll` / `WSAPoll`) yüzlerce bağlantıyı aynı anda yönetir, bağlantı başına thread açılmaz.
- **Otomatik failover:** Ulaşılamayan bir sunucu geçici olarak devre dışı bırakılır, istemci fark etmeden bir sonraki sağlıklı sunucuya yönlendirilir.
- **Asenkron bağlantı kurulumu:** Arka plan sunucusuna yapılan `connect()` çağrısı bile olay döngüsünü kilitlemez; zaman aşımı kontrolü de yine döngü içinde yapılır.
- **TCP half-close desteği:** Bir taraf bağlantıyı kapattığında tampondaki son bayt da karşıya iletilir, ardından FIN düzgün şekilde aktarılır.
- **Canlı istatistikler:** Her sunucu için toplam/aktif/başarısız bağlantı sayısı ve iki yönde aktarılan bayt miktarı thread-safe olarak okunabilir.
- **Platform bağımsız:** Platforma özel kodun tamamı `SocketUtils` katmanında; motorun kendisinde tek bir `#ifdef` bile yok.

---

## Hızlı Başlangıç

### Gereksinimler

- C++17 destekleyen bir derleyici (GCC 8+, Clang 7+, MSVC 2019+ veya MinGW-w64)
- CMake 3.16 veya üzeri

### Derleme

```bash
git clone https://github.com/erencicekk25/tcp-load-balancer.git
cd tcp-load-balancer
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

> **Windows notu:** Proje klasörünün yolunda Türkçe karakter (ş, ı, ğ vb.) varsa MinGW Makefiles üreticisi derleme sırasında hata verebilir. Bu durumda projeyi ASCII karakterli bir dizine taşımanız ya da `-G Ninja` / Visual Studio üreticisini kullanmanız yeterli.

### Testler

```bash
ctest --test-dir build --output-on-failure
```

Test paketi harici bir kütüphaneye ihtiyaç duymuyor. Round-Robin zamanlayıcıyı birim testleriyle kontrol ettikten sonra, loopback üzerinde rastgele portlarda gerçek backend'ler ayağa kaldırıp yük dengeleyiciyi uçtan uca deniyor:

- Bağlantıların backend'lere gerçekten `alpha → beta → gamma → alpha ...` sırasıyla dağıtılması
- Kapalı bir backend'in otomatik olarak atlanması (failover) ve sağlıksız olarak işaretlenmesi
- 1 MB'lık bir verinin iki yönde de bayt bayt bozulmadan taşınması ve sayaçların doğru tutulması
- `stop()` çağrısından sonra olay döngüsünün hızla kapanması

GitHub Actions her push'ta bu testleri Linux, macOS ve Windows'ta, ayrıca Linux'ta AddressSanitizer + UndefinedBehaviorSanitizer ile çalıştırıyor.

### Çalıştırma

Demo uygulaması; 3 adet sahte backend sunucusunu, önlerindeki yük dengeleyiciyi ve trafik üreten bir test istemcisini **aynı anda** ayağa kaldırır:

```bash
./build/tcp-load-balancer
```

Bir sunucunun çöktüğü senaryoyu görmek için:

```bash
./build/tcp-load-balancer --requests 6 --failover
```

Örnek çıktı:

```text
[15:12:34.061] [balancer ] session #1: 127.0.0.1:51503 -> 127.0.0.1:9001
[15:12:34.061] [client   ] request #1 -> backend-1 (port 9001) handled: request #1
[15:12:34.138] [balancer ] session #2: 127.0.0.1:51506 -> 127.0.0.1:9002
[15:12:34.139] [client   ] request #2 -> backend-2 (port 9002) handled: request #2
[15:12:34.223] [balancer ] session #3: 127.0.0.1:51512 -> 127.0.0.1:9003
[15:12:34.223] [client   ] request #3 -> backend-3 (port 9003) handled: request #3
...
[15:12:34.473] [main     ] simulating an outage: stopping backend-3
...
[15:12:36.683] [balancer ] session #9: backend 127.0.0.1:9003 unreachable (connect timeout), failing over
[15:12:36.683] [balancer ] session #9: 127.0.0.1:51524 -> 127.0.0.1:9001

=== Backend statistics (reported by the load balancer) ===
Endpoint          Status       Total  Active  Failed    Bytes in   Bytes out
----------------------------------------------------------------------------
127.0.0.1:9001    UP               5       0       0          56         211
127.0.0.1:9002    UP               5       0       0          57         212
127.0.0.1:9003    DOWN             2       0       1          22          84
```

Elle denemek isterseniz interaktif modu açıp `nc` veya `telnet` ile bağlanabilirsiniz. Her yeni bağlantının farklı bir sunucuya düştüğünü göreceksiniz:

```bash
./build/tcp-load-balancer --interactive
# başka bir terminalde:
nc 127.0.0.1 8080
```

| Parametre | Açıklama | Varsayılan |
|---|---|---|
| `--port <n>` | Yük dengeleyicinin dinleyeceği port | `8080` |
| `--backend-port <n>` | İlk backend sunucusunun portu | `9001` |
| `--backends <n>` | Simüle edilecek sunucu sayısı (1-16) | `3` |
| `--requests <n>` | Her turda gönderilecek istek sayısı | `9` |
| `--failover` | Son sunucuyu durdurup ikinci bir tur trafik gönderir | kapalı |
| `--interactive` | Elle test için açık kalır, ENTER ile kapanır | kapalı |

---

## Yük Dengeleme Nedir?

Tek bir sunucunun kaldırabileceği trafiğin bir sınırı vardır. Kullanıcı sayısı arttıkça ya sunucuyu büyütürsünüz (dikey ölçekleme) ya da aynı işi yapan birden fazla sunucuyu yan yana koyarsınız (yatay ölçekleme). İkinci yolu seçtiğiniz anda şu soru doğar: **Gelen bir isteği hangi sunucuya göndereceğim?**

Yük dengeleyici tam olarak bu soruyu cevaplayan katmandır. İstemciler tek bir adrese bağlanır; yük dengeleyici de bu bağlantıyı arkadaki sunuculardan birine aktarır. Bu sayede:

- Trafik sunucular arasında paylaşılır, hiçbir makine tek başına boğulmaz.
- Bir sunucu çöktüğünde trafik diğerlerine kayar, sistem ayakta kalır.
- Arkaya yeni sunucu eklemek ya da birini bakıma almak istemcileri hiç etkilemez.

### L4 ve L7 farkı

Yük dengeleyiciler genellikle OSI modelinde hangi katmanda çalıştıklarına göre ikiye ayrılır:

| | **L4 (Taşıma Katmanı)** | **L7 (Uygulama Katmanı)** |
|---|---|---|
| Neye bakar? | IP adresi ve port | HTTP başlıkları, URL, cookie vb. |
| Paketin içeriğini anlar mı? | Hayır, baytları olduğu gibi taşır | Evet, protokolü ayrıştırır |
| Hız | Çok hızlı, düşük gecikme | Görece daha yavaş |
| Örnek | AWS NLB, HAProxy (TCP modu) | Nginx, AWS ALB, Envoy |

Bu proje bir **L4 yük dengeleyicidir.** Trafiğin HTTP mi, Redis mi, yoksa özel bir protokol mü olduğuyla ilgilenmez; TCP üzerinden ne gelirse karşı tarafa aynen iletir.

---

## Yük Dengeleme Algoritmaları

Hangi isteğin hangi sunucuya gideceğine karar veren mantığa *yük dengeleme algoritması* denir. En yaygın olanları şunlar:

### 1. Round-Robin (bu projede kullanılan)

Sunucular bir halka gibi düşünülür ve her yeni bağlantı sıradakine verilir: 1 → 2 → 3 → 1 → 2 → 3 ...

```text
İstek:    #1   #2   #3   #4   #5   #6
Sunucu:   S1   S2   S3   S1   S2   S3
```

- **Artısı:** Son derece basit, durum tutmaz, öngörülebilir ve O(1) karmaşıklıkta.
- **Eksisi:** Sunucuların kapasitesini ya da o anki yükünü hesaba katmaz. Uzun süren bağlantılar bir sunucuda birikebilir.
- **Ne zaman uygun?** Sunucuların donanımı benzer ve isteklerin maliyeti birbirine yakınsa.

Bu projedeki uygulamada klasik Round-Robin'e iki küçük ama önemli dokunuş var:

1. **Sağlık kontrolü ile atlama:** O an "soğuma" (cooldown) süresindeki sunucular sıra onlara gelse bile atlanır.
2. **İyimser son şans:** Tüm sunucular aynı anda işaretlenmişse, bağlantıyı reddetmek yerine yine de sırayla denenir; belki biri çoktan toparlanmıştır.

### 2. Weighted Round-Robin (Ağırlıklı Round-Robin)

Her sunucuya bir ağırlık verilir. Ağırlığı 3 olan sunucu, ağırlığı 1 olana göre üç kat daha fazla bağlantı alır. Farklı güçte makinelerin olduğu ortamlar için idealdir.

### 3. Least Connections (En Az Bağlantı)

Yeni bağlantı, o an en az aktif bağlantıya sahip sunucuya gönderilir. Bağlantı sürelerinin çok değişken olduğu durumlarda (örneğin WebSocket veya veritabanı bağlantıları) Round-Robin'den daha adil sonuç verir.

### 4. IP Hash

İstemcinin IP adresinin hash değeri hesaplanır ve sonuç bir sunucuya eşlenir. Aynı istemci hep aynı sunucuya düşer. Oturum bilgisinin sunucu tarafında tutulduğu (*sticky session*) uygulamalar için kullanılır.

### 5. Least Response Time / Random / Power of Two Choices

- **Least Response Time:** En hızlı cevap veren sunucuyu seçer.
- **Random:** Rastgele seçim yapar; büyük sayılarda şaşırtıcı derecede iyi dağılır.
- **Power of Two Choices:** Rastgele iki sunucu seçer, bunlardan daha az yüklü olana gönderir. Basitliği ve verimliliği sayesinde modern sistemlerde oldukça popülerdir.

| Algoritma | Durum tutar mı? | Sunucu yükünü dikkate alır mı? | Karmaşıklık |
|---|---|---|---|
| Round-Robin | Sadece bir sayaç | Hayır | O(1) |
| Weighted Round-Robin | Sayaç + ağırlıklar | Kısmen (statik) | O(1) |
| Least Connections | Bağlantı sayıları | Evet | O(n) |
| IP Hash | Hayır | Hayır | O(1) |
| Power of Two Choices | Bağlantı sayıları | Evet | O(1) |

Kodda zamanlayıcı (`RoundRobinScheduler`) ayrı bir sınıf olarak tasarlandı. Yani ileride başka bir algoritmayı eklemek, motorun geri kalanına dokunmadan mümkün.

---

## Mimari: Trafik Nasıl Yönlendiriliyor?

### Genel bakış

```text
                          ┌──────────────────────────────────────┐
                          │            LoadBalancer              │
 ┌──────────┐   TCP       │                                      │      ┌────────────┐
 │ Client A │────────────▶│  listener ──accept()──▶ Session #1 ──┼─────▶│ backend-1  │
 └──────────┘             │                │                     │      └────────────┘
 ┌──────────┐             │                ▼                     │      ┌────────────┐
 │ Client B │────────────▶│      RoundRobinScheduler ──────▶ #2 ─┼─────▶│ backend-2  │
 └──────────┘             │                                      │      └────────────┘
 ┌──────────┐             │       poll() / WSAPoll() olay        │      ┌────────────┐
 │ Client C │────────────▶│       döngüsü (tek thread)     #3 ───┼─────▶│ backend-3  │
 └──────────┘             └──────────────────────────────────────┘      └────────────┘
```

### Bir bağlantının yaşam döngüsü

1. **Kabul (accept):** Dinleyici soket okunabilir hale geldiğinde, kuyruktaki bağlantılar tek seferde (en fazla 64 adet) kabul edilir ve non-blocking moda alınır. Bu sınır, yoğun bir bağlantı patlamasının mevcut oturumları aç bırakmasını engeller.
2. **Sunucu seçimi:** `RoundRobinScheduler` sıradaki uygun sunucuyu döndürür.
3. **Asenkron bağlantı:** Seçilen sunucuya non-blocking `connect()` başlatılır. Oturum `Connecting` durumuna geçer ve soket "yazılabilir" olayı için izlenir. Bağlantının sonucu `SO_ERROR` ile kontrol edilir.
4. **Failover:** Bağlantı reddedilirse ya da zaman aşımına uğrarsa sunucu belirli bir süre için "sağlıksız" olarak işaretlenir ve aynı istemci için sıradaki sunucu denenir. İstemci bu süreçten haberdar bile olmaz.
5. **Veri aktarımı (`Streaming`):** Her oturumun iki yönlü tamponu vardır:
   - `upstream` : istemci → sunucu
   - `downstream`: sunucu → istemci
6. **Kapanış:** Taraflardan biri bağlantıyı kapattığında tampondaki veri bitene kadar karşıya yazılmaya devam edilir, ardından `shutdown(SHUT_WR)` ile FIN iletilir. İki yön de kapandığında oturum temizlenir.

```text
             ┌────────────┐   connect OK    ┌────────────┐   iki yönde FIN   ┌──────────┐
 accept() ──▶│ Connecting │────────────────▶│ Streaming  │──────────────────▶│ Finished │
             └────────────┘                 └────────────┘                   └──────────┘
                   │  hata / timeout              │ okuma-yazma hatası            ▲
                   ▼                              └───────────────────────────────┘
          sıradaki sunucuyu dene
       (hiçbiri yoksa → Finished)
```

### Neden non-blocking ve tek thread?

Klasik yaklaşımda her bağlantı için bir thread açılır. Az sayıda bağlantıda sorun olmaz ama binlerce bağlantıda bellek ve bağlam değiştirme (context switch) maliyeti hızla katlanır.

Bu projede tüm soketler non-blocking moddadır ve tek bir olay döngüsü, hangi soketin okumaya ya da yazmaya hazır olduğunu `poll()` ile işletim sistemine sorar. Nginx, Redis ve Node.js'in temelinde yatan **reactor deseni** de budur.

Döngünün her turunda yapılanlar kısaca şöyle:

```text
while (running) {
    1. Dinleyici soketi + her oturumun soketlerini ilgi alanlarıyla birlikte listeye ekle
       - upstream tamponunda yer varsa      → istemciden OKU
       - downstream tamponunda veri varsa   → istemciye YAZ
       - (aynı mantık sunucu soketi için de)
    2. poll() ile hazır soketleri bekle
    3. Hazır olan her soket için oku/yaz, okunan veriyi hemen karşıya iletmeyi dene
    4. Zaman aşımına uğrayan bağlantı denemelerini failover'a gönder
    5. Biten oturumları temizle
}
```

### Geri basınç (backpressure)

Hızlı bir gönderici ile yavaş bir alıcı arasında tamponun sonsuza kadar büyümemesi gerekir. Burada çözüm basit: Bir yöndeki tampon dolduğunda o yönün kaynağı artık okuma için izlenmez. Böylece TCP'nin kendi akış kontrolü devreye girer ve gönderici doğal olarak yavaşlar. Bellek kullanımı oturum başına sabit kalır (varsayılan olarak yön başına 64 KB).

### Platform soyutlaması

Windows ve POSIX soket API'leri birbirine çok benzese de detaylarda ayrışır: `closesocket` / `close`, `WSAPoll` / `poll`, `WSAEWOULDBLOCK` / `EAGAIN`, `SO_EXCLUSIVEADDRUSE` / `SO_REUSEADDR` gibi. Bu farkların tamamı `SocketUtils` modülünde toplandı; yük dengeleyici motoru yalnızca bu sade arayüzle konuşuyor.

---

## Proje Yapısı

```text
tcp-load-balancer/
├── CMakeLists.txt          # C++17 derleme yapılandırması
├── include/
│   ├── LoadBalancer.hpp    # Motorun public arayüzü, config ve istatistik tipleri
│   └── SocketUtils.hpp     # Platformdan bağımsız soket soyutlaması
├── src/
│   ├── LoadBalancer.cpp    # Olay döngüsü, Round-Robin, failover, veri aktarımı
│   ├── SocketUtils.cpp     # Winsock / POSIX implementasyonu
│   └── main.cpp            # Simüle backend'ler + test istemcisi içeren CLI demo
├── tests/
│   └── LoadBalancerTests.cpp  # Zamanlayıcı birim testleri + uçtan uca testler
└── .github/workflows/ci.yml   # Linux / macOS / Windows + sanitizer CI
```

### Motoru kendi projenizde kullanmak

```cpp
#include "LoadBalancer.hpp"

int main() {
    tlb::net::NetworkContext network;

    tlb::LoadBalancerConfig config;
    config.listenPort = 8080;
    config.backends = {
        {"10.0.0.11", 9000},
        {"10.0.0.12", 9000},
        {"10.0.0.13", 9000},
    };
    config.logger = [](const std::string& message) { std::puts(message.c_str()); };

    tlb::LoadBalancer balancer(config);
    balancer.start();
    balancer.run();  // stop() çağrılana kadar bloklar
}
```

---

## Yapılandırma

| Alan | Açıklama | Varsayılan |
|---|---|---|
| `listenHost` / `listenPort` | Dinlenecek adres | `127.0.0.1:8080` |
| `backends` | Arka plan sunucularının listesi | — |
| `connectTimeout` | Sunucuya bağlanma için azami süre | `2000 ms` |
| `backendCooldown` | Hatalı sunucunun atlanacağı süre | `5000 ms` |
| `bufferSize` | Oturum başına, yön başına tampon boyutu | `64 KB` |
| `pollIntervalMs` | `poll()` için azami bekleme süresi | `100 ms` |
| `logger` | İsteğe bağlı log fonksiyonu | — |

---

## Bilinen Sınırlamalar ve Yol Haritası

Bu proje öğrenme amaçlı başladı ama üzerine eklenebilecek çok şey var. Aklımdaki bazı adımlar:

- [ ] Linux'ta `epoll`, BSD/macOS'ta `kqueue` desteği (şu an `poll()` her turda O(n) çalışıyor)
- [ ] Weighted Round-Robin ve Least Connections algoritmaları
- [ ] Arka planda periyodik aktif sağlık kontrolleri (health check)
- [ ] YAML/JSON dosyasından yapılandırma okuma
- [ ] Çok çekirdekli kullanım için `SO_REUSEPORT` ile birden fazla olay döngüsü
- [ ] Prometheus formatında metrik endpoint'i
- [ ] Birim ve entegrasyon testleri

Fikirleriniz ya da katkılarınız varsa issue veya pull request açmaktan çekinmeyin.

---

## Lisans

Bu proje [MIT Lisansı](LICENSE) ile lisanslanmıştır.

---

**Etiketler:** `#cpp` `#cpp17` `#modern-cpp` `#load-balancer` `#round-robin` `#tcp` `#tcp-proxy` `#networking` `#network-programming` `#socket-programming` `#non-blocking-io` `#event-loop` `#reactor-pattern` `#systems-programming` `#distributed-systems` `#cross-platform` `#cmake` `#winsock` `#backend` `#yuk-dengeleme`
