"""
SAP IBP Data Integration Flask Application
Main application entry point
"""
from flask import Flask
from config import Config
from routes import extraction_bp, import_bp, health_bp
from master_data_routes import master_data_bp
from xyz_segmentation_routes import xyz_segmentation_bp

def create_app(config_class=Config):
    """Application factory pattern"""
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Validate configuration on startup
    if not Config.validate():
        print("⚠️  Starting with incomplete configuration...")
    
    # Register blueprints
    app.register_blueprint(health_bp)
    app.register_blueprint(extraction_bp, url_prefix='/api/extract')
    app.register_blueprint(import_bp, url_prefix='/api/import')
    app.register_blueprint(master_data_bp, url_prefix='/api/master-data')
    app.register_blueprint(xyz_segmentation_bp, url_prefix='/api/xyz-segmentation')
    
    return app

if __name__ == '__main__':
    app = create_app()
    
    print("\n" + "=" * 60)
    print("🚀 SAP IBP Integration Service Starting...")
    print("=" * 60)
    print(f"Host: {app.config['HOST']}")
    print(f"Port: {app.config['PORT']}")
    print(f"Debug: {app.config['DEBUG']}")
    print(f"SAP IBP Host: {Config.SAP_IBP_HOST}")
    print(f"Username Set: {bool(Config.SAP_IBP_USERNAME)}")
    print(f"Password Set: {bool(Config.SAP_IBP_PASSWORD)}")
    print("\nAvailable API Endpoints:")
    print("  Health Check:        /health")
    print("  Connection Test:     /test-connection")
    print("  Key Figures:         /api/extract/*, /api/import/*")
    print("  Master Data:         /api/master-data/*")
    print("  XYZ Segmentation:    /api/xyz-segmentation/*")
    print("=" * 60 + "\n")
    
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        debug=app.config['DEBUG']
    )