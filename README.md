# JavaScript Streams examples

Working revision / Arbeitsstand: `2026-11-26.4`.

Small examples of Node.js streams and Web Streams. Run the commands from this
directory with Node.js 24. The examples use synthetic data and built-in APIs.
The optional framework comparison and PostgreSQL export have separate npm
dependencies.

Kleine Beispiele zu Node.js-Streams und Web Streams. Führe die Befehle aus
diesem Verzeichnis mit Node.js 24 aus. Die Beispiele nutzen synthetische Daten
und eingebaute APIs. Der optionale Framework-Vergleich und der PostgreSQL-Export
haben eigene npm-Abhängigkeiten.

| Example / Beispiel | Run / Aufruf | Expected result / Erwartetes Ergebnis |
| --- | --- | --- |
| File pipeline with `gzip` / Dateipipeline mit `gzip` | `node examples/03-gzip.mjs fixtures/events.ndjson \| gzip -dc` | Original NDJSON records / ursprüngliche NDJSON-Datensätze |
| Backpressure | `node examples/04-backpressure.mjs` | `write bb false`, then / danach `drain` |
| HTTP echo / HTTP-Echo | `node examples/08-http-echo.mjs` | Local server listens on / lokaler Server lauscht auf `127.0.0.1:3000` |
| UTF-8 decoding / UTF-8-Dekodierung | `node examples/10-web-decode.mjs` | Two decoded chunks / zwei dekodierte Chunks: `"Gr"` and / und `"üße\n"` |

## Core tests / Kern-Tests

From this directory, run the core tests with Node.js 24. The quoted patterns
select only the suites that run without npm packages. The HTTP tests bind to
free ports on the loopback interface.

Die Kern-Tests laufen aus diesem Ordner mit Node.js 24. Die Dateimuster in
Anführungszeichen wählen die Tests aus, die keine npm-Pakete voraussetzen.
Die HTTP-Tests belegen freie Ports auf der Loopback-Schnittstelle.

~~~sh
node --test "test/*.test.mjs" "projects/event-pipeline/test/*.test.mjs"
~~~

## Chapters 01–03 / Kapitel 01–03

When you return to the examples in a new terminal, open the directory that
contains `examples/` and `fixtures/`. Check with `node --version` that you are
using Node.js 24.

Öffne beim Wiedereinstieg in einem neuen Terminal den Ordner mit `examples/`
und `fixtures/`. Prüfe mit `node --version`, dass du Node.js 24 verwendest.

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S01 | Count the same bytes with full buffering and streaming / Dieselben Bytes gepuffert und als Stream zählen | `node examples/01-buffer-vs-stream.mjs buffer fixtures/events.ndjson` and / und `node examples/01-buffer-vs-stream.mjs stream fixtures/events.ndjson` |
| S01 contrast / Gegenprobe | Count bytes or keep all chunks / Bytes zählen oder alle Chunks behalten | `node examples/01-retained-bytes.mjs count fixtures/events.ndjson` or / oder `retain` |
| S01-G | Generate synthetic UTF-8 NDJSON / Synthetische UTF-8-NDJSON-Daten erzeugen | `node examples/01-generate-fixture.mjs 3` |
| S02 | Count bytes and observe stream events / Bytes zählen und Stream-Ereignisse beobachten | `node examples/02-readable-writable.mjs` |
| S02-F | Copy a file into a temporary directory / Datei in ein temporäres Verzeichnis kopieren | `node examples/02-file-copy.mjs` |
| S02-F verification / Inhaltsprüfung | Compare the bytes of the small fixture and its copy / Bytes der kleinen Beispieldatei und der Kopie vergleichen | `node examples/02-file-copy.mjs --verify` |
| S02 ownership / Besitz | Reuse backing storage or copy the payload / Speicher wiederverwenden oder Nutzdaten kopieren | `node examples/02-buffer-lifetime.mjs view` (also / auch `copy`) |
| S02 Writable units / Schreibqueue-Einheiten | Compare one string measured in bytes and code units / Einen String in Bytes und Codeeinheiten vergleichen | `node examples/02-writable-units.mjs` |
| S02-M | Compare how a readable delivers chunks / Lesestile vergleichen | `node examples/02-read-modes.mjs data` (also / auch `readable`, `iterate`) |
| S02-P | Observe a writable being connected and disconnected / Anschluss und Trennung an der Senke beobachten | `node examples/02-pipe-events.mjs` |
| S02-O | Pass event objects through custom streams / Ereignisobjekte durch eigene Streams führen | `node examples/02-object-mode.mjs` |
| S03 | Compress and decompress in the shell / In der Shell komprimieren und dekomprimieren | `node examples/03-gzip.mjs fixtures/events.ndjson \| gzip -dc` |
| S03-E | Compare success, source failure and sink failure / Erfolg, Quellenfehler und Senkenfehler vergleichen | `node examples/03-pipeline-errors.mjs ok` (also / auch `source`, `sink`) |
| S03-F | Compress to a temporary path and rename after success / Temporär komprimieren und nach Erfolg umbenennen | `node examples/03-gzip-file.mjs fixtures/events.ndjson scratch/events.gz` |
| S03-V | Verify the decompressed bytes / Dekomprimierte Bytes prüfen | `node examples/03-check-gzip.mjs fixtures/events.ndjson scratch/events.gz` |
| S03-F contrast / Gegenprobe | Fail at the final rename / Fehler beim letzten Umbenennen auslösen | `node examples/03-rename-failure.mjs` |
| Export order / Exportreihenfolge | Compare the published versions with and without a version check / Veröffentlichte Versionen mit und ohne Versionsprüfung vergleichen | `node examples/03-export-order.mjs` |

The S03 pipe uses the external `gzip` command. For a Node-only Gzip round trip,
run S03-F and then S03-V once S03-F succeeds. Create `scratch/` first. A successful S03-F
run can replace its output file. S03-V prints `sameBytes=170` for the supplied
fixture. If removal of the temporary directory fails, S03-F prints a separate
`cleanup warning` on standard error. The exit status reports whether compression
and the final rename succeeded.

Die Pipe in S03 verwendet das externe Programm `gzip`. Für einen Gzip-Rundlauf
mit Node.js allein führe S03-F aus und starte S03-V nach einem erfolgreichen Lauf. Lege zuerst
`scratch/` an. Ein erfolgreicher S03-F-Lauf kann die Zieldatei ersetzen.
S03-V meldet für die mitgelieferte Datei `sameBytes=170`. Scheitert das
Entfernen des temporären Verzeichnisses, schreibt S03-F eine separate
`cleanup warning` auf die Fehlerausgabe. Der Exitstatus zeigt, ob
Komprimierung und Umbenennen erfolgreich waren.

~~~sh
node -e "require('node:fs').mkdirSync('scratch', { recursive: true })"
~~~

## Pilot chapters / Pilotkapitel

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S04 | The second write reaches the threshold / Der zweite Write erreicht die Schwelle | `node examples/04-backpressure.mjs` |
| S04 terminal contrast / Gegenprobe | Observe a rejected write / Abgelehnten Write beobachten | `node examples/04-backpressure.mjs --after-end` or / oder `--after-destroy` |
| S04-G | Wait for drain when a sink closes early / Bei vorzeitigem Schließen auf drain warten | `node examples/04-drain-close.mjs drain` or / oder `close` |
| S04 queue length / Queue-Länge | Compare pending bytes before and after a write / Ausstehende Bytes vor und nach einem Write vergleichen | `node examples/04-backpressure.mjs 3 --length` |
| S04-Q | Same bytes, different queue peaks / Gleiche Datenmenge, verschiedene Höchstwerte | `node examples/04-queue-compare.mjs respect 10` and / und `node examples/04-queue-compare.mjs ignore 10` |
| S04-R | Pull from a custom readable and observe four queues / Eigene Quelle und vier Warteschlangen beobachten | `node examples/04-readable-pipeline.mjs` |
| S04-C | At most two tasks in flight / Höchstens zwei laufende Aufgaben | `node examples/04-bounded-work.mjs` |
| S10-D | Decode UTF-8 across a byte boundary / UTF-8 über eine Chunkgrenze dekodieren | `node examples/10-web-decode.mjs` |
| Web message pipeline / Web-Pipeline für Textmeldungen | Decode, frame, and display two NDJSON messages / Zwei NDJSON-Meldungen dekodieren, abgrenzen und anzeigen | `node examples/10-web-messages.mjs` or / oder `node examples/10-web-messages.mjs --trace` |
| S10-Q | Measure queue capacity and hand off a reader / Queue-Kapazität messen und den Reader wechseln | `node examples/10-web-queues.mjs 4 bytes --fresh` (also / auch `chunks`) |
| S10-W | Writer readiness before a write finishes / Schreibbereitschaft vor Abschluss eines Writes | `node examples/10-web-writer.mjs` |
| S10-L | Normal close, cancellation, write failure, and malformed UTF-8 / Reguläres Ende, Abbruch, Schreibfehler und fehlerhaftes UTF-8 | `node examples/10-web-lifecycle.mjs` |
| S10-B | Observe byte-stream buffer ownership / Pufferbesitz im Web Byte Stream beobachten | `node examples/10-web-byob.mjs` |

S04-Q accepts a sink delay between 0 and 250 milliseconds. Both modes process
32 bytes. The queue peaks at 8 bytes in `respect` mode and at 32 bytes in
`ignore` mode. Pass `6` to S04 to try a six-byte threshold. S04-R reports
total elapsed time and accepts a sink delay from 0 to 250 milliseconds.
Add `--fail` to S04-C to see status 1 after all work in the failing batch
has finished.

S04-Q akzeptiert für die Senke eine Verzögerung von 0 bis 250 Millisekunden. Beide
Varianten verarbeiten 32 Byte. Die Warteschlange erreicht 8 Byte mit `respect`
und 32 Byte mit `ignore`. Das Argument `6` stellt in S04 die Pufferschwelle
auf sechs Byte. S04-R misst die Gesamtlaufzeit und nimmt eine Verzögerung
von 0 bis 250 Millisekunden an. Mit `--fail` endet S04-C mit Status 1,
sobald alle Aufgaben der betroffenen Gruppe beendet sind.

Pass `5` to S10-D to split the bytes of ß, or to S10-Q to set a five-byte
threshold. In the browser, use the module queries `?split=5` and `?threshold=5`.
Compare `4 bytes --fresh` with `4 chunks --fresh` in S10-Q to change the
accounting unit and observe a fresh source.
S10-L also accepts `--replacement` and `--keep-sink` in Node.js, or
`?replacement=1` and `?keepSink=1` in the browser.

Das Argument `5` trennt in S10-D die Bytes von ß oder setzt in S10-Q die
Schwelle auf fünf Byte. Im Browser lauten die Modulparameter `?split=5`
und `?threshold=5`. Vergleiche `4 bytes --fresh` und `4 chunks --fresh` in
S10-Q, um die Zähleinheit zu wechseln und eine neue Quelle zu beobachten.
S10-L akzeptiert `--replacement` und `--keep-sink` in
Node.js sowie `?replacement=1` und `?keepSink=1` im Browser.

## Chapters 05–07 / Kapitel 05–07

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S05 | Filter objects and serialize NDJSON / Objekte filtern und als NDJSON ausgeben | `node examples/05-transform-objects.mjs warn` |
| S05-D | Observe the two sides of a duplex / Zwei Seiten eines Duplex beobachten | `node examples/05-duplex.mjs` |
| S05-B | Emit zero, one, or several outputs / Null, einen oder mehrere Werte ausgeben | `node examples/05-transform-burst.mjs` |
| S05-A | Compare early and ordered callbacks / Frühe und geordnete Bestätigung vergleichen | `node examples/05-transform-callback.mjs` |
| S06 | Cancel a staged write and keep the previous result / Temporären Lauf abbrechen und vorherige Ausgabe erhalten | `node examples/06-abort.mjs abort scratch/result.txt` |
| S06-W | Observe completion, early close, and a cancelled wait / Ende, vorzeitigen Schluss und abgebrochenes Warten beobachten | `node examples/06-finished.mjs` |
| S06-S | Track late effects and keep the active run's result / Verspätete Wirkung verfolgen und Ergebnis des aktiven Laufs beibehalten | `node examples/06-side-work.mjs` |
| Asynchronous disposal / Asynchrone Freigabe | Inspect the abort and the disposal error together / Abbruch und Freigabefehler gemeinsam auswerten | `node examples/06-disposal-error.mjs` |
| S07 | Parse bounded NDJSON lines across chunk boundaries / Begrenzte NDJSON-Zeilen über Chunkgrenzen lesen | `node examples/07-ndjson.mjs fixtures/07-events.ndjson 7 256` |
| S07-P | Compare competing errors and final lines at different cuts / Konkurrierende Fehler und Schlusszeilen bei anderen Schnitten vergleichen | `node examples/07-ndjson-probe.mjs` |
| S07-I | Compare iterator cleanup on early exit / Aufräumen beim frühen Schleifenende vergleichen | `node examples/07-iterator-close.mjs` |
| S07-U | Decode UTF-8 across a chunk boundary / UTF-8 über eine Chunkgrenze dekodieren | `node examples/07-utf8.mjs` |

S05 writes data to standard output and the record counts to standard error.
Add `--invalid` after the level to test a transform failure. Run S05-B with
`--slow` to observe the effect of a slower sink.

S05 schreibt die Datensätze auf die Standardausgabe und die Statistik auf die
Fehlerausgabe. Füge nach dem Level `--invalid` hinzu, um den Transform-Fehler zu
testen. Starte S05-B mit `--slow` und beobachte eine langsamere Senke.

For S06, create `scratch/` and a new sample file containing `previous`.
If the path is taken, choose an unused name for all commands. Run
`abort`, `timeout`, and `error` separately. Read each exit status (1)
before checking that the file still contains `previous`. Then use `ok`
to replace the sample file with the two new lines.

Lege für S06 zuerst `scratch/` und eine neue Beispieldatei mit `previous` an.
Ist der Name belegt, verwende für alle Befehle einen unbenutzten Pfad.
Starte `abort`, `timeout` und `error` einzeln. Lies jeweils den Exitstatus (1)
ab und prüfe danach, ob in der Datei weiter `previous` steht.
Mit `ok` ersetzt du die Beispieldatei anschließend durch die beiden neuen Zeilen.

~~~sh
node -e "require('node:fs').mkdirSync('scratch', { recursive: true })"
node -e "require('node:fs').writeFileSync('scratch/result.txt', 'previous\n', { flag: 'wx' })"
~~~

The valid S07 fixture deliberately uses CRLF, a blank line and a final record
without LF. Chunk sizes 1, 7, and 64 yield the same three records. S07
forwards the original JSON text of each valid record with an LF ending.
Use `fixtures/07-invalid.ndjson` to see the error at physical line 3.
Run S07-U with `--incomplete` to observe the error when decoding a truncated character.

Die gültige S07-Datei enthält bewusst CRLF, eine Leerzeile und einen letzten
Datensatz ohne LF. Mit 1, 7 und 64 Byte pro Chunk entstehen dieselben drei
Datensätze. S07 gibt deren ursprünglichen JSON-Text mit LF als Zeilenende aus.
`fixtures/07-invalid.ndjson` zeigt den Fehler in physischer Zeile 3.
S07-U mit `--incomplete` zeigt den Decoderfehler bei einem unvollständigen Zeichen.

## Local HTTP echo / Lokales HTTP-Echo

With the echo server running, send a request from another terminal. The
response contains the request body byte for byte. Stop the server with Ctrl+C.

Sende bei laufendem Echo-Server aus einem zweiten Terminal eine Anfrage. Die
Antwort enthält den Request-Body Byte für Byte. Beende den Server mit Ctrl+C.

~~~sh
node examples/08-http-client.mjs echo
~~~

## Chapters 08–09 / Kapitel 08–09

The example servers listen on `127.0.0.1`. For the servers you start yourself,
run client commands in a second terminal and stop the servers with Ctrl+C.

Die Beispielserver lauschen auf `127.0.0.1`. Führe bei manuell gestarteten
Servern die Client-Befehle in einem zweiten Terminal aus. Strg+C beendet die
Server.

| ID | Experiment / Versuch | Start |
| --- | --- | --- |
| S08 | Binary echo / Binär-Echo on port 3000 | `node examples/08-http-echo.mjs` |
| S08-F | Download and bounded upload / Download und begrenzter Upload on port 3001 | `node examples/08-http-files.mjs` |
| S08-C | Disconnect after the first download bytes / Nach den ersten Download-Bytes trennen | `node examples/08-http-disconnect.mjs` |
| S08-U | Stall an upload, attempt a second and verify recovery / Einen Upload stocken lassen, zweiten versuchen und Freigabe prüfen | `node examples/08-http-upload-probe.mjs` |
| S09 | Serve JSON, NDJSON and SSE / JSON, NDJSON und SSE ausliefern on port 3002 | `node examples/09-progressive-http/server.mjs` |
| S09-M | Measure complete events / Vollständige Ereignisse messen | `node examples/09-progressive-http/measure.mjs ndjson` |
| S09-M-F | Replay the same SSE bytes at three chunk sizes / Dieselben SSE-Bytes mit drei Chunkgrößen prüfen | `node examples/09-progressive-http/framing-probe.mjs` |
| Bounded SSE replay / Begrenzte SSE-Wiederholung | Recover when the resume cursor is older than the retained events / Fortsetzen, wenn der Cursor älter als das Wiederholungsfenster ist | `node examples/09-progressive-http/replay-window.mjs` |

S08-F serves the fixture at `/download`. `/download-slow` sends synthetic binary
blocks, and `/download-fail` intentionally closes after a partial body. Upload
with `Content-Type: application/octet-stream` to `/upload`. The accepted
body is limited to 65,536 bytes and has a two-second deadline. Only one upload
can occupy the slot. On success the server can replace `scratch/08-upload/received.bin`.

S08-F liefert die Beispieldatei unter `/download`. `/download-slow` sendet
synthetische Binärblöcke, `/download-fail` endet absichtlich mit unvollständigem
Body. Sende `Content-Type: application/octet-stream` an `/upload`. Der Body
ist auf 65.536 Byte und zwei Sekunden begrenzt. Ein Upload kann den Platz
belegen. Bei Erfolg kann der Server
`scratch/08-upload/received.bin` ersetzen.

~~~sh
node examples/08-http-client.mjs download
node examples/08-http-client.mjs download-fail
node examples/08-http-client.mjs download-changed
node examples/08-http-client.mjs upload
node examples/08-http-disconnect.mjs
~~~

[Express and Fastify](examples/http-framework-disconnect/disconnect.test.mjs)
compare two responses after a real HTTP disconnect. With no signal in the
pending operation, the source produces a second record after the response
closes. With the signal, the operation aborts and the source releases its
resources. The tests start and stop the servers themselves and also
verify normal completion.

Bei [Express und Fastify](examples/http-framework-disconnect/disconnect.test.mjs)
trennt der Client eine echte HTTP-Verbindung. Ohne Signal an der wartenden
Operation erzeugt die Quelle nach dem Antwortschluss einen zweiten Datensatz.
Mit Signal bricht die Operation ab und die Quelle räumt auf. Die Tests starten
und beenden die Server selbst und prüfen auch das reguläre Ende.

~~~sh
npm ci --prefix examples/http-framework-disconnect
npm --prefix examples/http-framework-disconnect test
~~~

### S08-PG · PostgreSQL to CSV over HTTP / PostgreSQL als CSV über HTTP

The [probe](examples/http-postgres-export/probe.mjs) starts its HTTP server on
a free loopback port. It connects to a local PostgreSQL test database through
`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. It checks the CSV
bytes, an HTTP disconnect while a second cursor read is in flight, and a
follow-up query through a new pooled connection. Its data comes from
`generate_series`.

Die [Probe](examples/http-postgres-export/probe.mjs) startet ihren HTTP-Server
auf einem freien lokalen Port. Über `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`
und `PGDATABASE` erreicht sie eine lokale PostgreSQL-Testdatenbank. Sie
prüft die CSV-Bytes, den HTTP-Abbruch während des zweiten Cursorabrufs
und eine Folgeabfrage über eine neue Pool-Verbindung. Die Zeilen erzeugt
`generate_series`.

For example, this command starts a disposable database in the foreground.
Zum Beispiel startet dieser Befehl eine temporäre Datenbank im Vordergrund.

~~~sh
docker run --rm -p 127.0.0.1:55432:5432 -e POSTGRES_USER=streams_demo -e POSTGRES_PASSWORD=streams_demo -e POSTGRES_DB=streams_demo postgres:16-alpine
~~~

In a second terminal, after PostgreSQL reports that it is ready, run the probe
from this directory. Stop the database with Ctrl+C in the first terminal
after the probe. Set the equivalent environment variables in your shell if
it uses different syntax.

Starte die Probe aus diesem Verzeichnis in einem zweiten Terminal, sobald
PostgreSQL seine Bereitschaft meldet. Beende die Datenbank danach im ersten
Terminal mit Strg+C. Verwende bei anderer Shell-Syntax die gleichnamigen
Umgebungsvariablen deiner Shell.

~~~sh
npm ci --prefix examples/http-postgres-export
PGHOST=127.0.0.1 PGPORT=55432 PGUSER=streams_demo \
PGPASSWORD=streams_demo PGDATABASE=streams_demo \
npm --prefix examples/http-postgres-export run probe
~~~

The last line is `follow-up database=1 newConnection=true`. The export probe
was verified with PostgreSQL 16.14 and Node.js 24.22.3.
Die letzte Zeile lautet `follow-up database=1 newConnection=true`. Geprüft mit
PostgreSQL 16.14 und Node.js 24.22.3.

### S09-M · Response formats / Antwortformate

For S09-M, run the client as `json`, `ndjson`, or `sse`. Use
`sse --reconnect` to resume at the last ID or `sse --replay-one` to see
duplicate ID 2 skipped. Cursor 4 retrieves completion event 5. Cursor 5
receives HTTP 204. Open http://127.0.0.1:3002/ and click **Start demo**
to observe the browser reconnect and close after validating the final event.
Use `/?without-end=1` in the browser or `sse --no-final` in the measuring client
for the counterexample.

S09-M akzeptiert `json`, `ndjson` und `sse`. Mit `sse --reconnect` setzt der
Client nach der letzten ID fort. Mit `sse --replay-one` verwirft er die
wiederholte ID 2. Cursor 4 erhält das Abschlussereignis 5. Cursor 5 erhält
HTTP 204. Öffne http://127.0.0.1:3002/ und klicke auf **Start demo**.
Der Browser verbindet sich erneut und schließt die Quelle nach Prüfung des
Finales. Verwende für den Gegenfall `/?without-end=1` im Browser oder
`sse --no-final` im Messclient.

~~~sh
node examples/09-progressive-http/measure.mjs sse --replay-one
~~~

Compare `sse --replay-conflict` with `--replay-one`. A repeat of ID 2 with
different content stops the measurement with status 1. The equivalent
browser scenario is `/?replay-conflict=1`.

Vergleiche `sse --replay-conflict` und `--replay-one`. Weicht der Inhalt
bei wiederholter ID 2 ab, endet die Messung mit Status 1. Dieselbe Variante
ist im Browser unter `/?replay-conflict=1` erreichbar.

Run `node examples/09-progressive-http/replay-window.mjs` in a second
terminal while `node examples/09-progressive-http/server.mjs` is running.
Cursor 2 receives IDs 3 through 5. Cursor 1 encounters a gap, loads the
confirmed snapshot through ID 4, and then receives the final event 5.

Starte `node examples/09-progressive-http/replay-window.mjs` in einem zweiten
Terminal, während `node examples/09-progressive-http/server.mjs` läuft. Auf
Cursor 2 folgen die IDs 3 bis 5. Bei Cursor 1 erkennt der Client eine Lücke,
lädt den bestätigten Stand bis ID 4 und erhält anschließend das Abschlussereignis 5.

## Chapters 11–12 / Kapitel 11–12

| ID | Experiment / Versuch | Start |
| --- | --- | --- |
| S11 | Display a Fetch response progressively and restart safely / Fetch-Antwort fortlaufend anzeigen und neu starten | `node examples/11-fetch-browser/server.mjs` |
| S11-U | Compare blob and streamed upload / Blob- und Stream-Upload vergleichen | In browser console / in der Browserkonsole: `await (await import('/upload.mjs')).probeUpload()` |
| HTTP 307 | Observe the requests received before and after an upload redirect / Empfangene Anfragen vor und nach der Upload-Umleitung beobachten | `node examples/11-fetch-upload-redirect.mjs` |
| S12 | Pass the same bytes through Node and Web streams / Dieselben Bytes durch Node und Web Streams leiten | `node examples/12-interop.mjs web fixtures/events.ndjson` |
| S12-V | Verify both paths against small text and binary fixtures / Beide Pfade gegen kleine Text- und Binärproben prüfen | `node examples/12-verify.mjs` |
| S12-D | Adapt duplex streams in both directions / Duplex-Streams in beide Richtungen adaptieren | `node examples/12-duplex.mjs from` or / oder `to` |
| S12-L | Follow success, failure and cancellation across adapters / Abschluss, Fehler und Abbruch über Adaptergrenzen verfolgen | `node examples/12-adapter-lifecycle.mjs ok`, `error` or / oder `abort` |
| S12-T | Compare backlogs and partial effects / Rückstand und Teilwirkungen vergleichen | `node examples/12-tee.mjs tee` (also / auch `paired`, `paired-fail`) |

For S11, open http://127.0.0.1:3003/. **Complete feed** shows six records.
**Short feed (clean EOF)** and the truncated response both show IDs 1 and 2.
Compare their terminal messages to distinguish valid framing from a partial line.
**Missing record ID** leaves the same two visible rows and reports an
unexpected record ID when record 4 follows 2.
Try **Abort**. Focus returns to **Start or restart**. Focus also returns there
if **Abort** is focused when the feed finishes or fails. Restart while a feed
is active, and select each error scenario. Compare **Fast feed** at the
default URL and `/?batch=1` to observe DOM update counts.
Read the count with `document.querySelector('#records').dataset.renderCommits`.
Both views retain IDs 13–24.
The optional upload probe measures the actual bytes received over HTTP/1.1.
In the tested Chrome and Firefox builds, the blob matched and the streamed
request returned `TypeError` against this local server.

Öffne für S11 http://127.0.0.1:3003/. **Complete feed** zeigt sechs
Datensätze. **Short feed (clean EOF)** und die abgeschnittene Antwort zeigen
beide die IDs 1 und 2. Vergleiche am Ende gültige Rahmung und Teilzeile.
**Missing record ID** lässt dieselben zwei Zeilen sichtbar und meldet eine
unerwartete ID, wenn Datensatz 4 auf Datensatz 2 folgt.
Probiere **Abort**. Der Fokus kehrt zu **Start or restart** zurück. Das gilt
auch, wenn **Abort** beim regulären Ende oder beim Fehler fokussiert ist.
Probiere auch den Neustart bei laufendem Feed und die Fehlerszenarien.
Vergleiche mit **Fast feed** an der Standardadresse und `/?batch=1` die Zahl
der DOM-Updates. Die optionale Upload-Probe prüft die tatsächlich empfangenen
Bytes über HTTP/1.1. In den getesteten Versionen von Chrome und Firefox
stimmte der Blob. Der Stream-Aufruf endete mit `TypeError`.
Den Zähler liest du mit `document.querySelector('#records').dataset.renderCommits`.
Beide Ansichten behalten die IDs 13–24.

S12 writes the original bytes to standard output in both `node` and `web`
mode. Redirect to a file when trying binary input. For S12-T, pass `32`
after `tee` or `paired` to try 32 blocks instead of the default 16.
With `paired-fail 8`, expect status 1 and the partial writes `fast=2` and `slow=1`.

S12 schreibt in `node` und `web` die Originalbytes auf die Standardausgabe.
Mit `32` nach `tee` oder `paired` probierst du in S12-T 32 Blöcke aus.
Mit `paired-fail 8` erwartest du Status 1 und die Teilwirkungen `fast=2` und `slow=1`.
Leite binäre Ausgaben zum Vergleich in eine Datei um.

## Chapters 13–14 / Kapitel 13–14

Run `node benchmarks/13-concurrent-imports.mjs` for the concurrent import
comparison from Section 13.5. It reports admission, wait time, and memory
for the same verified work in both configurations.

`node benchmarks/13-concurrent-imports.mjs` startet die Vergleichsmessung für
gleichzeitige Importe aus Abschnitt 13.5. Beide Varianten erledigen dieselbe
geprüfte Arbeit. Die Ausgabe zeigt Zulassung, Wartezeit und Speicher.

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S13 | Measure equivalent work with three read and write policies / Dieselbe Arbeit mit drei Lese- und Schreibstrategien messen | `node benchmarks/13-stream-baselines.mjs 5` |
| S13-S | Summarize duration, throughput, RSS, and queue peaks / Dauer, Durchsatz, RSS und Queue-Spitzen auswerten | `node benchmarks/13-summarize.mjs scratch/bench-13.ndjson` |
| S13-S paired differences / Paardifferenzen | Compare `unpaced − stream` within each trial / `unpaced − stream` je Wiederholung vergleichen | `node benchmarks/13-summarize.mjs --pairs scratch/bench-13.ndjson` |
| S13-M | Compare storage retained by views and copies after the queue drains / Gehaltene Speicherbereiche bei leerer Schreibqueue vergleichen | `node examples/13-retained-views.mjs view` and / und `node examples/13-retained-views.mjs copy` |
| S14 | Observe source and sink delays, malformed input, and cancellation / Quellen- und Senkenverzögerung, ungültige Eingabe und Abbruch beobachten | `node examples/14-observe-pipeline.mjs ok` |
| S14-S | Summarize one run per log file / Einen Lauf pro Protokolldatei zusammenfassen | `node examples/14-summarize.mjs scratch/14-ok.ndjson` |
| S14-D | Prepare three blind diagnostic cases / Drei Diagnosefälle ohne Szenarionamen vorbereiten | `node examples/14-prepare-diagnosis.mjs` |
| Completion cohort / Ausgänge einer Laufgruppe | Compare an early and a later reading of the same cohort / Abschlüsse derselben Laufgruppe zu zwei Zeitpunkten auswerten | `node examples/14-completion-cohort.mjs` |

~~~sh
node -e "require('node:fs').mkdirSync('scratch', { recursive: true })"
node benchmarks/13-stream-baselines.mjs 5 > scratch/bench-13.ndjson
node benchmarks/13-summarize.mjs scratch/bench-13.ndjson
node examples/14-observe-pipeline.mjs ok > scratch/14-ok.ndjson
node examples/14-summarize.mjs scratch/14-ok.ndjson
~~~

S13 generates fixed 1 MiB and 8 MiB files in a temporary directory, checks
the byte count and SHA-256 for each run, and emits 60 measured samples with
five trials. Use `--queue-blocks=2` or `--cpu-rounds=32` in separate control
runs. Add `--loop-delay` to sample delays in the event loop.
The `unpaced` mode deliberately ignores backpressure for these fixed inputs.

S13 erzeugt feste Dateien mit 1 MiB und 8 MiB in einem temporären Verzeichnis,
prüft Bytezahl und SHA-256 jedes Laufs und gibt bei fünf Wiederholungen
60 Messstichproben aus. Verwende `--queue-blocks=2` oder
`--cpu-rounds=32` in getrennten Kontrollläufen. Mit `--loop-delay`
misst du zusätzlich Verzögerungen
der Event Loop. `unpaced` ignoriert Backpressure absichtlich für diese festen
Eingaben.

S13-S reports medians and the minimum and maximum total duration for a
complete matrix. Missing or
duplicate samples, or different content within a file size, cause status 1.
Use a POSIX shell or Windows Command Prompt for `>`. Choose a new name
to preserve an earlier recording.

S13-S zeigt bei einer vollständigen Matrix Mediane sowie Minimum und Maximum
der Gesamtlaufzeit.
Fehlende oder doppelte Stichproben sowie abweichende Inhalte je Dateigröße
führen zu Status 1. Verwende für `>` eine POSIX-Shell oder die
Windows-Eingabeaufforderung. Ein neuer Dateiname erhält frühere Aufzeichnungen.

S14 also accepts `slow-sink`, `slow-source`, `bad-record`, and `abort`.
Run `bad-record` and `abort` separately. Both write their end event and
exit with status 1. Redirect each run to a separate file. S14-S expects
one ordered run with a shared ID and valid start and end events per file.
It checks the terminal status, reason, and counters before printing. When
several files are supplied, every file must pass and have a distinct run ID.

S14 akzeptiert auch `slow-sink`, `slow-source`, `bad-record` und `abort`.
Führe `bad-record` und `abort` einzeln aus. Beide schreiben ihr
Abschlussereignis und enden mit Status 1. Leite jeden Lauf in eine eigene
Datei um. S14-S erwartet pro Datei einen geordneten Lauf mit gemeinsamer ID
sowie gültiges Start- und Abschlussereignis. Status, Grund und Zähler werden
vor der Ausgabe geprüft. Bei mehreren Dateien muss jede die Prüfung
bestehen und eine eigene Lauf-ID haben.

S14-D prints a directory with `case-1.ndjson` through `case-3.ndjson` and
a separate answer key. Change only the sink delay for a control run with
`node examples/14-observe-pipeline.mjs slow-sink --sink-ms=6`.

S14-D gibt einen Ordner mit `case-1.ndjson` bis `case-3.ndjson` und
einer getrennten Auflösung aus. Im Kontrolllauf mit
`node examples/14-observe-pipeline.mjs slow-sink --sink-ms=6` änderst du
nur die Wartezeit der Senke.

## Chapters 15–16 / Kapitel 15–16

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S15 | Test boundaries, cancellation, and cleanup / Grenzen, Abbruch und Aufräumen testen | `node --test test/15-pipeline.test.mjs` |
| S15-K | Verify a five-byte contract / Einen Vertrag für fünf Bytes prüfen | `node --test test/15-small-limit.test.mjs` |
| S16 | Run the local capstone / Lokales Abschlussprojekt starten | `node projects/event-pipeline/server.mjs` |
| S16-A | Run the file pipeline without a server / Dateikette ohne Server starten | `node projects/event-pipeline/run-file.mjs demo` |
| S16-H | Read progress from a second terminal / Fortschritt in einem zweiten Terminal lesen | `node projects/event-pipeline/run-client.mjs slow` |
| S16-H application check / Fachprüfung | Check the terminal application status / Fachliches Abschlussereignis auswerten | `node projects/event-pipeline/run-client.mjs bad --check` |
| S16-I | Capture three local incidents / Drei lokale Vorfälle aufzeichnen | `node projects/event-pipeline/capture-incident.mjs` |
| S16-R | Capture three outcomes of a service restart / Drei Ausgänge eines Dienstneustarts aufzeichnen | `node projects/event-pipeline/capture-restart.mjs` |
| S16-G | Generate synthetic NDJSON / Synthetisches NDJSON erzeugen | `node projects/event-pipeline/generate.mjs 12` |
| S16-V | Check expanded bytes, SHA-256, and the IDs of the small fixture / Entpackte Bytes, SHA-256 und die IDs der kleinen Beispieldatei prüfen | `node projects/event-pipeline/check.mjs "OUTPUT_PATH" --demo` |
| S16-T | Test the pipeline through HTTP / Die Pipeline über HTTP testen | `node --test projects/event-pipeline/test/event-pipeline.test.mjs` |
| S16 protocol checks / Protokollprüfung | Test incomplete and inconsistent responses without a server / Unvollständige und widersprüchliche Antworten ohne Server testen | `node --test projects/event-pipeline/test/progress.test.mjs` |

Start S16 and open [the local demo](http://127.0.0.1:3004/). For scenarios,
expected results, and verification commands, see the
[Event Pipeline README](projects/event-pipeline/README.md).

Starte S16 und öffne [die lokale Demo](http://127.0.0.1:3004/). Szenarien,
Sollwerte und Prüfkommandos stehen in der
[Anleitung zur Event Pipeline](projects/event-pipeline/README.md).

`node projects/event-pipeline/capture-process-restart.mjs` records the
POSIX process restart from Section 16.5 using local servers. The command
checks the old download after the first process has exited.

`node projects/event-pipeline/capture-process-restart.mjs` zeichnet den
POSIX-Prozesswechsel aus Abschnitt 16.5 mit lokalen Servern auf. Der Aufruf
prüft den alten Download, nachdem der erste Prozess beendet ist.

## Stream patterns / Stream-Patterns

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S-B9 | Bound active work and preserve output order / Nebenläufige Arbeit begrenzen und Ergebnisse geordnet ausgeben | `node examples/b-ordered-concurrency.mjs` |
| S-B10 | Keep only the latest update for an optional display / Nur den neuesten Stand für eine optionale Anzeige behalten | `node examples/b-optional-progress.mjs` |

S-B9 holds the first record until the window is full. Run `abort` or `error`
for exit status 1 and cleanup, or `early-exit` to close the iterator after
two results. S-B10 returns the main result while the display is paused.
In `sink-error` and `disconnect`, the main result still succeeds.

S-B9 hält den ersten Datensatz zurück, bis das Fenster voll ist. `abort` und
`error` zeigen Fehler und Aufräumen mit Exitcode 1. `early-exit` beendet den
Iterator nach zwei Ergebnissen. S-B10 meldet das Ergebnis der Hauptverarbeitung,
während die Anzeige wartet. Auch bei `sink-error` und `disconnect` ist die
Hauptverarbeitung erfolgreich.

Run the pattern tests / Pattern-Tests starten

~~~sh
node --test test/appendix-b-patterns.test.mjs
~~~

## Troubleshooting appendix / Anhang Fehlerdiagnose

| ID | Experiment / Versuch | Run / Aufruf |
| --- | --- | --- |
| S-C1 | Compare an acknowledged write with a missing callback / Bestätigten Write und fehlenden Callback vergleichen | `node examples/c-pipeline-stall.mjs complete` or / oder `node examples/c-pipeline-stall.mjs stall` |

The `stall` mode admits one object, records one pending write, then aborts
the pipeline. It deliberately reports `ABORT_ERR` on standard error and exits
with status 1.

`stall` nimmt ein Objekt an, meldet einen ausstehenden Write und bricht die
Pipeline anschließend ab. Der Modus meldet `ABORT_ERR` auf der Fehlerausgabe
und endet absichtlich mit Exitcode 1.

## Browser modules / Browsermodule

The S10 examples use Web APIs and can run as browser modules. Serve this
folder on localhost, open http://127.0.0.1:8000/, and import a module in the
page’s developer console.

Die S10-Beispiele nutzen Web-APIs und laufen als Browsermodule. Stelle
diesen Ordner lokal bereit, öffne http://127.0.0.1:8000/ und importiere ein
Modul in der Entwicklerkonsole.

~~~sh
python3 -m http.server 8000 --bind 127.0.0.1
~~~

~~~js
await import('/examples/10-web-decode.mjs');
~~~

Reload the page to run the same module again. / Lade die Seite neu, um
dasselbe Modul erneut auszuführen.
