# Fermenterübersicht und Wiederaufnahme

Die Startansicht zeigt alle registrierten Fermenter mit Online-Status,
Temperatur, Sollwert, Dichte, Modus, Profil und Alarmstatus. Die Detailansicht
bleibt über die Karte und die Navigation erreichbar.

## MQTT-Vertrag

- `fermentorcontrol/devices` (retained):
  `{ "schemaVersion": 1, "devices": [{ "id": "F01", "name": "F01", "online": true }] }`
- Die Liste ist maßgeblich für Navigation und Übersicht. Eine leere Liste
  entfernt alle Geräte aus der Anzeige. Einzelzustände unbekannter Geräte
  fügen kein Gerät hinzu; alte retained Zustände erzeugen daher keine Geistergeräte.
- Neue Live-MQTT-Meldungen eines entfernten Controllers registrieren ihn im
  Fermentor-Control-Backend wieder. Dessen neue Geräteliste fügt ihn in MBFC hinzu.
- Die browserlokalen Kurzzeitverläufe bleiben beim Entfernen erhalten und werden
  beim Wiederauftauchen geladen. Das vollständige Chargenarchiv liegt weiterhin
  in der MongoDB von Fermentor Control.
- Ohne das neue Gerätelist-Topic dienen die bisherigen Profilarchiv-Targets als
  Übergangslösung. Nach Empfang der Geräteliste können alte Archiv-Targets die
  aktive Liste nicht überschreiben. Die neue Liste benötigt kein Profil-Leserecht.
- Der MBFC-MQTT-Benutzer benötigt Subscribe-Rechte für `fermentorcontrol/devices`
  sowie die bereits verwendeten Zustands-, Availability- und Profiltopics.

MBFC bleibt auf GitHub Pages. Fermentor Control läuft auf dem Raspberry Pi.
Die GitHub-Pages-Veröffentlichung erfolgt über den vorhandenen Workflow nach
Übernahme der Änderungen in `main`. Es werden keine Brokerzugangsdaten eingecheckt.

Prüfung: `node --check web/app.js` und `node --test tests/*.test.cjs`.
