"""PGSync plugin."""
import importlib
import inspect
import logging
import os
import pkgutil
import typing
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Plugin base class."""

    @abstractmethod
    def transform(self, doc, **kwargs):
        """Must be implemented by all derived classes."""
        pass


class Plugins(object):
    def __init__(self, package, names=None):
        self.package = package
        self.names = names or []
        self.reload()
        sorted(self.plugins, key=lambda plugin: self.names.index(plugin.name))
        logger.info(f"plugins will run in this order: {list(map(lambda plugin: plugin.name, self.plugins))}")

    def reload(self):
        """Reload the plugins from the available list."""
        self.plugins = []
        self._paths = []
        logger.debug(f"Reloading plugins from package: {self.package}")
        self.walk(self.package)
        self.plugins = sorted(self.plugins, key=lambda plugin: self.names.index(plugin.name))
        logger.info(f"plugins will run in this order: {list(map(lambda plugin: plugin.name, self.plugins))}")

    def walk(self, package):
        """Recursively walk the supplied package and fetch all plugins"""
        plugins = importlib.import_module(package)
        for _, name, ispkg in pkgutil.iter_modules(
            plugins.__path__,
            f"{plugins.__name__}.",
        ):
            if ispkg:
                continue

            module = importlib.import_module(name)
            members = inspect.getmembers(module, inspect.isclass)
            for _, klass in members:
                if issubclass(klass, Plugin) & (klass is not Plugin):
                    if klass.name not in self.names:
                        continue
                    logger.debug(
                        f"Plugin class: {klass.__module__}.{klass.__name__}"
                    )
                    self.plugins.append(klass())

        paths = []
        if isinstance(plugins.__path__, str):
            paths.append(plugins.__path__)
        else:
            paths.extend([path for path in plugins.__path__])

        for pkg_path in paths:

            if pkg_path in self._paths:
                continue

            self._paths.append(pkg_path)
            for pkg in [
                path
                for path in os.listdir(pkg_path)
                if os.path.isdir(os.path.join(pkg_path, path))
            ]:
                self.walk(f"{package}.{pkg}")

    def transform(self, docs):
        """Apply all plugins to each doc."""
        for doc in docs:
            skip_doc = False

            try:
                for plugin in self.plugins:
                    logger.debug(f"Plugin: {plugin.name}")

                    dx = plugin.transform(
                        doc["_source"],
                        _id=doc["_id"],
                        _index=doc["_index"],
                        _fulldoc=doc,
                    )

                    if isinstance(dx, (typing.List, typing.Tuple)):
                        for item in dx:
                            docs.append(item)
                        skip_doc = True
                        break
                    elif dx is None:
                        skip_doc = True
                    else:
                        doc["_source"] = dx
            except Exception as e:
                logger.exception("Plugin or data problem")
                logger.warning(f"Continuing on skipping document: {doc}")
                skip_doc = True

            if skip_doc:
                continue

            yield doc
