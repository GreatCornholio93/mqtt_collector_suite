﻿#######################################################################################################################
 _______  _______ __________________   _______  _______  _        _        _______  _______ _________ _______  _______ 
(       )(  ___  )\__   __/\__   __/  (  ____ \(  ___  )( \      ( \      (  ____ \(  ____ \\__   __/(  ___  )(  ____ )
| () () || (   ) |   ) (      ) (     | (    \/| (   ) || (      | (      | (    \/| (    \/   ) (   | (   ) || (    )|
| || || || |   | |   | |      | |     | |      | |   | || |      | |      | (__    | |         | |   | |   | || (____)|
| |(_)| || |   | |   | |      | |     | |      | |   | || |      | |      |  __)   | |         | |   | |   | ||     __)
| |   | || | /\| |   | |      | |     | |      | |   | || |      | |      | (      | |         | |   | |   | || (\ (   
| )   ( || (_\ \ |   | |      | |     | (____/\| (___) || (____/\| (____/\| (____/\| (____/\   | |   | (___) || ) \ \__
|/     \|(____\/_)   )_(      )_(     (_______/(_______)(_______/(_______/(_______/(_______/   )_(   (_______)|/   \__/

#######################################################################################################################

===================
INFO:             =
===================
- Nejaktuálnější verze je dostupná na: https://github.com/GreatCornholio93/mqtt_collector_suite
- Ve složce kde je rovněž i setup.py použít příkaz pip install . - to nainstaluje potřebné moduly
- Aplikace pro archivaci dat je ve složce MQQT_collector (main.py)
- Aplikace pro přístup k datům je dostupná ve složce mqtt_client_app (mqttt.py)
- Konfigurační soubory (MQTT_collector/config) jsou nastaveny na funkční broker a databázový server
===================

======================================
Ukázkové spouštění klientské aplikace:
======================================
# vizualizace grafu 
python mqttt.py -h tanas.eu -t stats/testing get-graph -b Testingbroker -r benchmark/rand-num --ops regression
python mqttt.py -h tanas.eu -t stats/testing get-graph -b Testingbroker -r benchmark/rand-num --ops regression --start 2018-04-26-19:37:05 --end 2018-04-26-19:46:13
python mqttt.py -h tanas.eu -t stats/testing get-graph -b Testingbroker -r benchmark/rand-num --ops regression --last 100M

# ziskani statistik
python mqttt.py -h tanas.eu -t stats/testing get-graph -b Testingbroker -r benchmark/rand-num --ops regression
python mqttt.py -h tanas.eu -t stats/testing get-graph -b Testingbroker -r benchmark/rand-num --ops regression --start 2018-04-26-19:37:05 --end 2018-04-26-19:46:13
python mqttt.py -h tanas.eu -t stats/testing get-graph -b Testingbroker -r benchmark/rand-num --ops regression --last 100M