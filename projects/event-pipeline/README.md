# Event Pipeline

A local capstone for JavaScript Streams. The server reads a synthetic NDJSON
file, drops debug records, publishes Gzip output, and reports progress over
HTTP. Run the commands from the `javascript-streams` directory with Node.js 24.
The output directory needs a local file system with hard-link support.

Lokales Abschlussprojekt für JavaScript Streams. Der Server liest synthetisches
NDJSON, filtert Debug-Datensätze, veröffentlicht Gzip und meldet Fortschritt
über HTTP. Führe die Befehle im Ordner `javascript-streams` mit Node.js 24 aus.
Der Ausgabeordner braucht ein lokales Dateisystem mit Hardlink-Unterstützung.

## A · Process a local file / Eine lokale Datei verarbeiten

~~~sh
node projects/event-pipeline/run-file.mjs demo
~~~

Use the printed `output` path in the verification command below. The expected
result is 12 read, 8 kept, 4 dropped, and 356 bytes after decompression. Run
`run-file.mjs invalid` for `invalid_json` at line 4, or `run-file.mjs demo 24`
for `line_limit` at line 1. Each invocation uses a fresh local directory.

Setze den ausgegebenen `output`-Pfad in den Prüfbefehl unten ein. Erwartet
sind 12 gelesene, 8 vom Filter ausgewählte und 4 verworfene Datensätze sowie 356 Byte
nach dem Entpacken. `run-file.mjs invalid` zeigt `invalid_json` in Zeile 4,
`run-file.mjs demo 24` zeigt `line_limit` in Zeile 1. Jeder Aufruf verwendet
ein neues lokales Verzeichnis.

~~~sh
node projects/event-pipeline/check.mjs "OUTPUT_PATH" --demo
~~~

## B · Follow HTTP progress / Fortschritt über HTTP verfolgen

~~~sh
node projects/event-pipeline/server.mjs
~~~

Run the client in a second terminal. The HTTP status describes the response.
The final NDJSON event gives the outcome of the job. Copy the run ID from the
successful end event into the file verification command below.

Starte den Client in einem zweiten Terminal. Der HTTP-Status beschreibt die
Antwort. Das letzte NDJSON-Ereignis liefert den Ausgang der Verarbeitung.
Setze die Lauf-ID aus dem erfolgreichen Abschlussereignis in die Dateiprüfung
unten ein.

~~~sh
node projects/event-pipeline/run-client.mjs slow
~~~

The client normally reports transport success. Append `--check` for the same
protocol validation used by the browser, with exit status 1 on a failed
application run or an unconfirmed final event. Compare `bad` and
`bad --check`. Both receive HTTP 200 and report `invalid_json` at line 4.
Their exit statuses are 0 and 1, respectively.

Der Client meldet standardmäßig den Übertragungserfolg. Mit `--check` prüft
er dasselbe Protokoll wie der Browser. Ein fachlicher Fehler oder ein
unbestätigter Abschluss führt zu Exitstatus 1. Vergleiche `bad` und
`bad --check`. Beide erhalten HTTP 200 und melden `invalid_json` in Zeile 4.
Die Exitstatus sind 0 beziehungsweise 1.

## C · Run in the browser / Im Browser ausführen

Open [the local demo](http://127.0.0.1:3004/) and start **Slow sink** to see
**Processing** before **Complete**. The link and checksum appear after the
browser checks the successful end event and the end of the response. Start
another slow run and choose **Abort** while it is processing. Wait for the
matching `run.end` in the terminal before starting again. A previous run
can still be cleaning up when the browser already shows **Aborted in browser**.
**Malformed record** stops at line 4.

Öffne [die lokale Demo](http://127.0.0.1:3004/) und starte **Slow sink**.
Die Ansicht zeigt **Processing** vor **Complete**. Link und Prüfsumme
erscheinen nach geprüftem Abschlussereignis und Ende der Antwort. Starte
**Slow sink** erneut und wähle währenddessen **Abort**. Warte vor dem
nächsten Start im Terminal auf `run.end` mit derselben Lauf-ID. Der
Server kann noch aufräumen, während die Ansicht schon **Aborted in browser**
meldet. **Malformed record** scheitert in Zeile 4.

A successful run on the demo file reads 12 records, keeps 8 and drops 4.
Replace `RUN_ID` below with the run ID from the end event. The expected SHA‑256
is for 356 bytes of normalized NDJSON after decompression.

Ein erfolgreicher Lauf liest 12 Datensätze. Der Filter wählt 8 aus und
verwirft 4. Ersetze unten `RUN_ID` durch die Lauf-ID aus dem
Abschlussereignis. Der erwartete SHA‑256-Wert gilt für 356 Byte normalisiertes
NDJSON nach dem Entpacken.

~~~sh
node projects/event-pipeline/check.mjs "scratch/event-pipeline/output/RUN_ID.ndjson.gz" fc127eb395903d16763493bfc297ccdf57f00bd91dde9c5c41876261ec5e26ef
~~~

## Compare three incidents / Drei Vorfälle vergleichen

~~~sh
node projects/event-pipeline/capture-incident.mjs
~~~

The printed temporary directory contains three independent cases with
`transfer.json`, `client.ndjson`, `server.ndjson`, and `output/`. Compare
the same run ID across the files, then verify a published result with
the checker's `--demo` mode. Chapter 16 explains the outcome of each case.
The script uses available local ports and stops its own servers.

Der ausgegebene temporäre Ordner enthält drei Fälle mit `transfer.json`,
`client.ndjson`, `server.ndjson` und `output/`. Gleiche zuerst dieselbe
Lauf-ID über die Dateien ab. Prüfe eine veröffentlichte Datei anschließend
mit `check.mjs` und `--demo`. Kapitel 16 erklärt den Ausgang jedes Falls.
Das Skript verwendet freie lokale Ports und beendet seine Server selbst.

## Service restart / Dienstneustart

~~~sh
node projects/event-pipeline/capture-restart.mjs
~~~

S16-R calls `server.shutdown({ graceMs })` while the old instance is
handling a run. The three cases cover completion within the grace period,
cancellation before publication, and cancellation with publication pending.
The replacement binds the same port and uses the same output directory in
the same process. It checks the previous download path before starting a
new run. Each case has a `summary.json` and separate client and server
logs in the temporary directory printed by the command.

S16-R ruft `server.shutdown({ graceMs })` für eine Instanz mit einem laufenden
Auftrag auf. Die drei Fälle zeigen Erfolg innerhalb der Frist, Abbruch vor
dem Dateiübergang und ein Abbruchsignal bei ausstehender Veröffentlichung.
Die Ersatzinstanz verwendet im selben Prozess denselben Port und
Ausgabeordner. Sie prüft den alten Downloadpfad vor dem nächsten Lauf.
Zu jedem Fall liegen `summary.json` sowie getrennte Client- und Serverlogs
im ausgegebenen temporären Ordner.

### Separate Node.js processes / Getrennte Node.js-Prozesse

~~~sh
node projects/event-pipeline/capture-process-restart.mjs
~~~

This POSIX probe runs two cases with separate server processes. The driver
sends `SIGTERM` with a file write paused, waits for the old process to exit,
and starts its replacement on the same loopback port and with the same
output directory. The old download gives HTTP 200 with verified content
when the write is released during shutdown. It gives `404 missing_output`
when the shutdown deadline cancels the write before publication. The
temporary evidence directory contains each case's `summary.json` and logs.

Diese POSIX-Probe führt zwei Fälle mit getrennten Serverprozessen aus.
Der Treiber sendet `SIGTERM` bei angehaltenem Datei-Write, wartet auf das
Ende des alten Prozesses und startet den Ersatzprozess am selben lokalen
Port mit demselben Ausgabeordner. Bei Freigabe während der Abschaltung
liefert der alte Download HTTP 200 mit geprüftem Inhalt. Beendet die
Abschaltfrist den Write vor der Veröffentlichung, folgt `404 missing_output`.
Der temporäre Belegordner enthält pro Fall `summary.json` und die Logs.

## Larger fixture / Größere Beispieldatei

Generate input and start a new run. Use a POSIX shell or Windows Command
Prompt for the redirection, which can replace the local synthetic input file.
The expected result is 50,000 read, 33,333 kept, and
16,667 dropped. The browser offers the same input under **Generated file**.

Erzeuge die Eingabe und starte einen neuen Lauf. Verwende für die Umleitung
eine POSIX-Shell oder die Windows-Eingabeaufforderung. Die Umleitung kann die
lokale synthetische Eingabedatei ersetzen. Der Lauf liest 50.000 Datensätze.
Der Filter wählt 33.333 aus und verwirft 16.667. Die Oberfläche bietet dieselbe
Eingabe unter **Generated file** an.

~~~sh
node -e "require('node:fs').mkdirSync('scratch/event-pipeline', { recursive: true })"
node projects/event-pipeline/generate.mjs 50000 > scratch/event-pipeline/input.ndjson
node projects/event-pipeline/run-client.mjs ok large
~~~

## Input, bounds, and tests / Eingabe, Grenzen und Tests

- Input is UTF‑8 NDJSON with a final LF. CRLF is accepted. Records have a
  positive safe-integer `id`, a `level` of `info`, `debug`, or `warn`, and
  an `event` string of at most 80 UTF‑16 code units. Output keeps these three
  fields and uses LF.
- Eingabe ist UTF‑8-NDJSON mit abschließendem LF. CRLF ist erlaubt.
  Datensätze enthalten eine positive sichere Ganzzahl als `id`, `info`,
  `debug` oder `warn` als `level` und einen `event`-String mit höchstens
  80 UTF‑16-Codeeinheiten. Die Ausgabe übernimmt diese Felder und nutzt LF.
- Each run admits up to 8 MiB, 512 bytes per line before LF, and 100,000
  records. The server handles one run at a time and signals a deadline after
  15 seconds. The verifier limits both compressed and expanded input to 16 MiB
  each.
- Pro Lauf sind bis zu 8 MiB, 512 Byte vor LF je Zeile und 100.000 Datensätze
  erlaubt. Der Server verarbeitet einen Lauf gleichzeitig und signalisiert nach
  15 Sekunden einen Fristablauf. Der Prüfer begrenzt komprimierte und
  expandierte Eingabe auf jeweils 16 MiB.
- Publication follows a successful pipeline and creates a download path for
  this run. Files in `scratch/event-pipeline/output/` remain until removed
  locally. A disconnect before submission of the publication step leaves no
  finished file. If a disconnect coincides with successful publication, use
  the server log to check the outcome. If cleanup fails as well, the log
  preserves the original result alongside `cleanupPending: true`.
- Nach erfolgreicher Pipeline veröffentlicht der Server einen Downloadpfad für
  diesen Lauf. Dateien in `scratch/event-pipeline/output/` bleiben erhalten,
  bis sie lokal entfernt werden. Ein Abbruch vor dem Publikationsschritt
  hinterlässt keine fertige Datei. Bei einem Abbruch während der Publikation
  zeigt das Serverlog, ob der Schritt erfolgreich war. Scheitert zusätzlich
  das Aufräumen, steht `cleanupPending: true` neben dem ursprünglichen Ergebnis.

~~~sh
node --test test/15-pipeline.test.mjs projects/event-pipeline/test/event-pipeline.test.mjs
node --test projects/event-pipeline/test/progress.test.mjs
~~~

The tests use isolated temporary directories. HTTP tests bind only to loopback
on an available port. Stop the demo server with Ctrl+C.

Die Tests nutzen separate temporäre Verzeichnisse. Die HTTP-Tests lauschen
nur auf Loopback an einem freien Port. Beende den Demoserver mit Strg+C.
