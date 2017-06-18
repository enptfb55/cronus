import imp
from job_handle import JobHandle

class JobHandleFactory:

    @staticmethod
    def create(config):
        if config.has_section('Plugins'):
            for name, path in config.items('Plugins'):
                print path.rsplit( ".", 1 )[0]
                imp.load_source(path.rsplit( ".", 1 )[0], path)

        handle_dict = {}
        for type in JobHandle.__subclasses__():
            handle_dict.update({type.__name__ : type})

        handle = handle_dict.get(config.get('Source', 'type'))

        if handle:
            return handle(config)

