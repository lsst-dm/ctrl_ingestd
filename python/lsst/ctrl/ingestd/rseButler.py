import logging
from lsst.daf.butler import Butler
from lsst.daf.butler.registry import CollectionType
from lsst.obs.base.ingest import RawIngestConfig, RawIngestTask
from lsst.pipe.base import Instrument

LOGGER = logging.getLogger(__name__)


class RseButler:

    def __init__(self, config):

        instrument = config["instrument"]
        collections = config["collections"]
        repo = config["repo"]
        try:
            self.butlerConfig = Butler.makeRepo(repo)
        except FileExistsError:
            self.butlerConfig = repo

        self.butler = self.createButler(instrument, collections)

        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        self.task = RawIngestTask(config=cfg,
            butler=self.butler,
            on_success=self.on_success,
            on_ingest_failure=self.on_ingest_failure,
            on_metadata_failure=self.on_metadata_failure)

    def createButler(self, instrument, collections):
        instr = Instrument.from_string(instrument)
        run = instr.makeDefaultRawIngestRunName()
        opts = dict(run=run, writeable=True, collections=collections)
        butler = Butler(self.butlerConfig, **opts)

        # Register an instrument.
        instr.register(butler.registry)

        return butler

    def ingest(self, uri):
        self.task.run([uri])

    def on_success(self, datasets):
        """Callback used on successful ingest. logs
        successful data ingestion status

        Parameters
        ----------
        datasets: `list`
            list of DatasetRefs
        """
        for dataset in datasets:
            LOGGER.info("file %s successfully ingested", dataset.path.ospath)

    def on_ingest_failure(self, exposures, exc):
        """Callback used on ingest failure. Used to log
        unsuccessful data ingestion status

        Parameters
        ----------
        exposures: `RawExposureData`
            exposures that failed in ingest
        exc: `Exception`
            Exception which explains what happened

        """
        for f in exposures.files:
            real_file = f.filename.ospath
            cause = self.extract_cause(exc)
            LOGGER.info(f'OIF: {real_file}: {cause}')
            print(f'OIF: {real_file}: {cause}')

    def on_metadata_failure(self, filename, exc):
        """Callback used on metadata extraction failure. Used to log
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ButlerURI`
            ButlerURI that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        real_file = filename.ospath

        cause = self.extract_cause(exc)
        LOGGER.info(f'OMDF: {real_file}: {cause}')
        print(f'OMDF: {real_file}: {cause}')

    def extract_cause(self, e):
        """extract the cause of an exception

        Parameters
        ----------
        e : `BaseException`
            exception to extract cause from

        Returns
        -------
        s : `str`
            A string containing the cause of an exception
        """
        if e.__cause__ is None:
            return None
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"


