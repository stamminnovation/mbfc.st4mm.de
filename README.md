# R.I.P. Remote

Statische Remote-Weboberfläche für den R.I.P. - RAPT Fermentercontroller. Die Anwendung wird als GitHub Page aus dem Ordner `web/` veröffentlicht und benötigt auf dem Hosting weder Node.js noch PHP noch eine Datenbank.

## Funktionen

- vorgeschaltete Kennwortabfrage für GitHub Pages
- MQTT over WebSockets (`wss://`)
- mehrere Fermenter
- Availability Online/Offline
- Temperatur, Sollwert, Stellgröße, HEAT/COOL
- PID/Fuzzy-Anzeige
- Dichte und Dichteänderung pro Tag
- Profilstatus, Profilwahl, Profil Start/Stop
- Betriebsmodi IDLE/TEMP/PROFILE
- Sollwert ändern
- Alarmanzeige und Alarmquittierung
- Netzwerk-/Firmwareinformationen
- browserlokaler Kurzzeitverlauf
- responsive Desktop-/Smartphone-Oberfläche

## GitHub Pages einrichten

1. Unter **Settings → Secrets and variables → Actions → New repository secret** ein Secret anlegen:

   ```text
   Name:  RIPRAPT_REMOTE_PASSWORD
   Value: <eigenes starkes Kennwort>
   ```

2. Unter **Settings → Pages → Build and deployment** die Quelle **GitHub Actions** auswählen.
3. Einen Commit nach `main` pushen oder den Workflow **Deploy R.I.P. Remote to GitHub Pages** manuell starten.
4. Der Workflow erzeugt beim Deployment aus dem Secret einen zufälligen Salt und einen PBKDF2-SHA-256-Hash mit 310.000 Iterationen. Das Klartextkennwort wird nicht in die Pages-Dateien oder das Repository geschrieben.
5. Die ausgegebene GitHub-Pages-URL öffnen und anmelden.

Fehlt `RIPRAPT_REMOTE_PASSWORD`, bricht der Workflow absichtlich ab.

## Verhalten der Anmeldung

Eine erfolgreiche Anmeldung wird nur in `sessionStorage` gespeichert. Sie gilt damit für den aktuellen Browser-Tab und geht beim Schließen des Tabs verloren. Über **Abmelden** wird die Sitzung sofort verworfen.

### Sicherheitsgrenze

GitHub Pages liefert eine statische Website öffentlich aus. Der Passwortdialog ist daher ein zusätzlicher Sichtschutz, aber **keine serverseitige Zugriffskontrolle**. Salt und Hash liegen zwangsläufig im ausgelieferten JavaScript und können offline analysiert werden.

Die eigentliche Sicherheitsgrenze muss deshalb weiterhin der MQTT-Broker bilden:

- ausschließlich `wss://` mit gültigem TLS-Zertifikat
- eigener MQTT-Benutzer für R.I.P. Remote
- starkes, vom Seitenkennwort unabhängiges MQTT-Kennwort
- ACL nur für die benötigten Controller-Topics

Beispielhafte ACL:

```text
user riprapt-web
topic read riprapt/+/out/#
topic write riprapt/+/in
```

## MQTT

Im Einstellungsdialog wird eine vollständige WebSocket-URL eingetragen, z. B.:

```text
wss://mqtt.example.com/mqtt
```

Pro Fermenter werden Input-Topic und Output-Root konfiguriert:

```text
Input:       riprapt/F01/in
Output Root: riprapt/F01/out
```

Die App verwendet die retained Topics `/status`, `/network`, `/control`, `/profile`, `/profiles/index`, `/alarm`, `/availability` und `/ack`. Kommandos werden nicht retained auf das Input-Topic publiziert.

## Dateien

```text
web/
├── index.html
├── auth.js
├── auth-config.js
├── app.js
├── styles.css
├── icon.svg
└── manifest.webmanifest

.github/workflows/deploy-pages.yml
```

`auth-config.js` im Repository enthält absichtlich keine gültige Konfiguration. Der GitHub-Actions-Workflow ersetzt die Datei nur im Deployment-Artefakt durch Salt und Hash.
