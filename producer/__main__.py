"""Permite ejecutar el producer con: python -m producer [opciones]"""

import sys

from producer.kafka_producer import main

sys.exit(main())
