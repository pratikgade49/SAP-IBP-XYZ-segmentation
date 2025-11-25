from app.services.sap_service import SAPService
from app.services.analysis_service import AnalysisService


def get_sap_service() -> SAPService:
    """Dependency for SAP service"""
    return SAPService()


def get_analysis_service() -> AnalysisService:
    """Dependency for analysis service"""
    return AnalysisService()