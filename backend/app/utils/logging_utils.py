"""Application-wide logging setup."""
import logging


def configure_logging(app):
    level = logging.DEBUG if app.debug or app.config.get("TESTING") else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("segmentation")
    log.setLevel(level)
    return log
