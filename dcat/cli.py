import os
from ConfigParser import SafeConfigParser

from paste.script.command import Command
import paste.proxy, paste.fixture
from paste.script.util.logging_config import fileConfig

class DCatCommand(Command):
    parser = Command.standard_parser(verbose=True)
    parser.add_option('-c', '--config', dest='config',
            default='development.ini', help='Config file to use (default: development.ini)')
    default_verbosity = 1
    group_name = 'dcat'

    def _load_config(self):
        from paste.deploy import appconfig
        if not self.options.config:
            msg = 'No config file supplied'
            raise self.BadCommand(msg)
        self.filename = os.path.abspath(self.options.config)
        try:
            fileConfig(self.filename)
        except Exception: pass
        self.config = appconfig('config:' + self.filename)
    