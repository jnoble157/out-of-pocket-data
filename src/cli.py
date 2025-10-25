"""
Command-line interface for the medical pricing data processing application.
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional
import click
from dotenv import load_dotenv

from .database import db_manager, supabase_manager
from .processor import DataProcessor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('medical_pricing.log')
    ]
)

logger = logging.getLogger(__name__)


@click.group()
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              help='Set the logging level')
def cli(log_level):
    """Medical Pricing Data Processing CLI"""
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))


@cli.command()
@click.option('--supabase-url', envvar='SUPABASE_URL', 
              help='Supabase project URL')
@click.option('--supabase-key', envvar='SUPABASE_KEY', 
              help='Supabase API key')
def init_db(supabase_url, supabase_key):
    """Initialize the database schema."""
    try:
        # Initialize database connections
        db_manager.initialize()
        
        # Test connection
        if not db_manager.test_connection():
            click.echo("❌ Database connection failed", err=True)
            sys.exit(1)
        
        # Execute schema
        schema_path = Path(__file__).parent.parent / 'schema.sql'
        if schema_path.exists():
            db_manager.execute_sql_file(str(schema_path))
            click.echo("✅ Database schema initialized successfully")
        else:
            click.echo("❌ Schema file not found", err=True)
            sys.exit(1)
        
        # Initialize Supabase if credentials provided
        if supabase_url and supabase_key:
            try:
                supabase_manager.initialize()
                click.echo("✅ Supabase client initialized")
            except Exception as e:
                click.echo(f"⚠️  Supabase initialization failed: {e}", err=True)
        
    except Exception as e:
        click.echo(f"❌ Database initialization failed: {e}", err=True)
        sys.exit(1)
    finally:
        db_manager.close()


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--batch-size', default=1000, help='Batch size for processing')
@click.option('--max-workers', default=4, help='Maximum number of worker threads')
@click.option('--hospital-metadata', help='JSON string with hospital metadata')
def process_file(file_path, batch_size, max_workers, hospital_metadata):
    """Process a single CSV or JSON file."""
    async def _process():
        try:
            # Initialize database
            db_manager.initialize()
            
            # Parse hospital metadata if provided
            metadata = None
            if hospital_metadata:
                import json
                metadata = json.loads(hospital_metadata)
            
            # Create processor
            processor = DataProcessor(batch_size=batch_size, max_workers=max_workers)
            
            # Process file
            result = await processor.process_file(file_path, metadata)
            
            # Display results
            click.echo(f"📊 Processing Results:")
            click.echo(f"  Facility ID: {result.facility_id}")
            click.echo(f"  Total Records: {result.total_records}")
            click.echo(f"  Successful: {result.successful_records}")
            click.echo(f"  Failed: {result.failed_records}")
            click.echo(f"  Processing Time: {result.processing_time:.2f}s")
            
            if result.errors:
                click.echo(f"  Errors: {len(result.errors)}")
                for error in result.errors[:5]:  # Show first 5 errors
                    click.echo(f"    - {error}")
                if len(result.errors) > 5:
                    click.echo(f"    ... and {len(result.errors) - 5} more errors")
            
            processor.close()
            
        except Exception as e:
            click.echo(f"❌ Processing failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(_process())


@cli.command()
@click.argument('directory_path', type=click.Path(exists=True, file_okay=False))
@click.option('--pattern', default='*.csv', help='File pattern to match')
@click.option('--batch-size', default=1000, help='Batch size for processing')
@click.option('--max-workers', default=4, help='Maximum number of worker threads')
def process_directory(directory_path, pattern, batch_size, max_workers):
    """Process all files in a directory matching the pattern."""
    async def _process():
        try:
            # Initialize database
            db_manager.initialize()
            
            # Create processor
            processor = DataProcessor(batch_size=batch_size, max_workers=max_workers)
            
            # Process directory
            results = await processor.process_directory(directory_path, pattern)
            
            # Display summary
            total_records = sum(r.total_records for r in results)
            total_successful = sum(r.successful_records for r in results)
            total_failed = sum(r.failed_records for r in results)
            total_time = sum(r.processing_time for r in results)
            
            click.echo(f"📊 Directory Processing Summary:")
            click.echo(f"  Files Processed: {len(results)}")
            click.echo(f"  Total Records: {total_records}")
            click.echo(f"  Successful: {total_successful}")
            click.echo(f"  Failed: {total_failed}")
            click.echo(f"  Total Time: {total_time:.2f}s")
            
            # Show individual file results
            for result in results:
                click.echo(f"  📁 {result.facility_id}: {result.successful_records}/{result.total_records} records")
            
            processor.close()
            
        except Exception as e:
            click.echo(f"❌ Directory processing failed: {e}", err=True)
            sys.exit(1)
        finally:
            db_manager.close()
    
    asyncio.run(_process())


@cli.command()
@click.option('--facility-id', help='Filter by facility ID')
@click.option('--state', help='Filter by state')
@click.option('--limit', default=10, help='Limit number of results')
def query_hospitals(facility_id, state, limit):
    """Query hospitals in the database."""
    try:
        db_manager.initialize()
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT facility_id, facility_name, city, state, last_updated FROM hospitals"
                params = []
                
                conditions = []
                if facility_id:
                    conditions.append("facility_id = %s")
                    params.append(facility_id)
                if state:
                    conditions.append("state = %s")
                    params.append(state)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += f" ORDER BY ingested_at DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                if not results:
                    click.echo("No hospitals found")
                    return
                
                click.echo(f"🏥 Hospitals ({len(results)} results):")
                for row in results:
                    click.echo(f"  {row['facility_id']} - {row['facility_name']}")
                    click.echo(f"    📍 {row['city']}, {row['state']}")
                    click.echo(f"    📅 Last Updated: {row['last_updated']}")
                    click.echo()
        
    except Exception as e:
        click.echo(f"❌ Query failed: {e}", err=True)
        sys.exit(1)
    finally:
        db_manager.close()


@cli.command()
@click.option('--facility-id', help='Filter by facility ID')
@click.option('--min-price', type=float, help='Minimum cash price')
@click.option('--max-price', type=float, help='Maximum cash price')
@click.option('--description', help='Search in description')
@click.option('--limit', default=10, help='Limit number of results')
def query_operations(facility_id, min_price, max_price, description, limit):
    """Query medical operations in the database."""
    try:
        db_manager.initialize()
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT mo.facility_id, h.facility_name, mo.description, 
                           mo.cash_price, mo.gross_charge, mo.negotiated_min, mo.negotiated_max,
                           mo.codes
                    FROM medical_operations mo
                    JOIN hospitals h ON mo.facility_id = h.facility_id
                """
                params = []
                conditions = []
                
                if facility_id:
                    conditions.append("mo.facility_id = %s")
                    params.append(facility_id)
                if min_price is not None:
                    conditions.append("mo.cash_price >= %s")
                    params.append(min_price)
                if max_price is not None:
                    conditions.append("mo.cash_price <= %s")
                    params.append(max_price)
                if description:
                    conditions.append("mo.description ILIKE %s")
                    params.append(f"%{description}%")
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY mo.cash_price DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                if not results:
                    click.echo("No operations found")
                    return
                
                click.echo(f"🏥 Medical Operations ({len(results)} results):")
                for row in results:
                    click.echo(f"  {row['description']}")
                    click.echo(f"    🏥 {row['facility_name']} ({row['facility_id']})")
                    
                    # Handle None values for prices
                    cash_price = f"${row['cash_price']:.2f}" if row['cash_price'] is not None else "N/A"
                    gross_charge = f"${row['gross_charge']:.2f}" if row['gross_charge'] is not None else "N/A"
                    click.echo(f"    💰 Cash: {cash_price} | Gross: {gross_charge}")
                    
                    # Handle None values for negotiated prices
                    if row['negotiated_min'] is not None and row['negotiated_max'] is not None:
                        negotiated = f"${row['negotiated_min']:.2f} - ${row['negotiated_max']:.2f}"
                    elif row['negotiated_min'] is not None:
                        negotiated = f"${row['negotiated_min']:.2f} - N/A"
                    elif row['negotiated_max'] is not None:
                        negotiated = f"N/A - ${row['negotiated_max']:.2f}"
                    else:
                        negotiated = "N/A - N/A"
                    click.echo(f"    📊 Negotiated: {negotiated}")
                    click.echo(f"    🏷️  Codes: {row['codes']}")
                    click.echo()
        
    except Exception as e:
        click.echo(f"❌ Query failed: {e}", err=True)
        sys.exit(1)
    finally:
        db_manager.close()


@cli.command()
def delete_all():
    """Delete all hospital and medical operation records."""
    try:
        db_manager.initialize()
        
        # Delete in correct order (operations first due to foreign key constraint)
        click.echo("🗑️  Deleting all medical operations...")
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM medical_operations")
                operations_deleted = cursor.rowcount
                conn.commit()  # Commit the transaction
        click.echo(f"   Deleted {operations_deleted} medical operations")
        
        click.echo("🗑️  Deleting all hospitals...")
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM hospitals")
                hospitals_deleted = cursor.rowcount
                conn.commit()  # Commit the transaction
        click.echo(f"   Deleted {hospitals_deleted} hospitals")
        
        click.echo("✅ All records deleted successfully!")
        
    except Exception as e:
        click.echo(f"❌ Delete failed: {e}", err=True)
        sys.exit(1)
    finally:
        db_manager.close()


@cli.command()
def test_connection():
    """Test database connection."""
    try:
        db_manager.initialize()
        
        if db_manager.test_connection():
            click.echo("✅ Database connection successful")
        else:
            click.echo("❌ Database connection failed", err=True)
            sys.exit(1)
        
    except Exception as e:
        click.echo(f"❌ Connection test failed: {e}", err=True)
        sys.exit(1)
    finally:
        db_manager.close()


if __name__ == '__main__':
    cli()
