# Fermenterübersicht und Wiederaufnahme

Die Startansicht zeigt alle registrierten Fermenter mit Online-Status,
Temperatur, Sollwert, Dichte, Modus, Profil und Alarmstatus. Die Detailansicht
bleibt über die Karte und die Navigation erreichbar.

## MQTT-Vertrag

- `<Basis-Topic>/devices` (retained):
  `{ "schemaVersion": 1, "devices": [{ "id": "F01", "name": "F01", "online": true }] }`
- Die Liste ist maßgeblich für Navigation und Übersicht. Eine leere Liste
  entfernt alle Geräte aus der Anzeige. Einzelzustände unbekannter Geräte
  fügen kein Gerät hinzu; alte retained Zustände erzeugen daher keine Geistergeräte.
- Neue Live-MQTT-Meldungen eines entfernten Controllers registrieren ihn im
  Fermentor-Control-Backend wieder. Dessen neue Geräteliste fügt ihn in MBFC hinzu.
- Die browserlokalen Kurzzeitverläufe bleiben beim Entfernen erhalten und werden
  beim Wiederauftauchen geladen. Das vollständige Chargenarchiv liegt weiterhin
  in der MongoDB von Fermentor Control.
- Profilarchiv-Targets werden nie als aktive Geräteliste verwendet: alte retained
  Archivdaten können entfernte Geräte enthalten. Bis zur gültigen Geräteliste
  bleibt die Übersicht leer und zeigt einen Hinweis. Das gilt auch nach einem
  MQTT-Reconnect. Die Geräteliste benötigt kein Profil-Leserecht.
- Der MBFC-MQTT-Benutzer benötigt Subscribe-Rechte für `<Basis-Topic>/devices`
  sowie die bereits verwendeten Zustands-, Availability- und Profiltopics.

## Topic-Konfiguration

Im Verbindungsdialog lässt sich der MQTT Basis-Topic ändern. In der JSON-Datei
für den Credentials-Import (Version 2) steht er neben `broker`:

```json
"topics": { "baseTopic": "fermentercontrol" }
```

Der Wert muss exakt mit `externalMqttBaseTopic` im Raspberry übereinstimmen.
Der aktuelle Backend-Standard ist `fermentercontrol`; alte MBFC-Konfigurationen
ohne `topics` behalten aus Kompatibilitätsgründen `fermentorcontrol`.
Die Beispieldateien enthalten den aktuellen Backend-Standard. Groß-/Kleinschreibung
ist relevant. Keine MQTT-Wildcards verwenden. Verschachtelte Basis-Topics sind erlaubt.
Alle Geräte-, Availability-, Archiv- und Befehlstopics werden daraus abgeleitet.

MBFC bleibt auf GitHub Pages. Fermentor Control läuft auf dem Raspberry Pi.
Die GitHub-Pages-Veröffentlichung erfolgt über den vorhandenen Workflow nach
Übernahme der Änderungen in `main`. Es werden keine Brokerzugangsdaten eingecheckt.

Prüfung: `node --check web/app.js` und `node --test tests/*.test.cjs`.
