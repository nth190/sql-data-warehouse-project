#!/bin/bash

# dbt Run Script for Data Warehouse
# Usage: ./run_dbt.sh [command] [options]

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Navigate to dbt directory
cd "$(dirname "$0")"

case "$1" in
    "deps")
        print_info "Installing dbt dependencies..."
        dbt deps
        print_success "Dependencies installed!"
        ;;
    
    "full-refresh")
        print_info "Running full refresh of all models..."
        dbt run --full-refresh
        print_success "Full refresh completed!"
        ;;
    
    "staging")
        print_info "Running staging models..."
        dbt run --select staging
        print_success "Staging models completed!"
        ;;
    
    "intermediate")
        print_info "Running intermediate models..."
        dbt run --select intermediate
        print_success "Intermediate models completed!"
        ;;
    
    "gold")
        print_info "Running gold models..."
        dbt run --select gold
        print_success "Gold models completed!"
        ;;
    
    "dims")
        print_info "Running dimension models..."
        dbt run --select dim_customers dim_products
        print_success "Dimension models completed!"
        ;;
    
    "facts")
        print_info "Running fact models..."
        dbt run --select fact_sales
        print_success "Fact models completed!"
        ;;
    
    "test")
        print_info "Running all tests..."
        dbt test
        print_success "Tests completed!"
        ;;
    
    "docs")
        print_info "Generating documentation..."
        dbt docs generate
        print_info "Serving documentation..."
        dbt docs serve
        ;;
    
    "all")
        print_info "Running complete pipeline..."
        print_info "Step 1/4: Installing dependencies..."
        dbt deps
        
        print_info "Step 2/4: Running all models..."
        dbt run
        
        print_info "Step 3/4: Running tests..."
        dbt test
        
        print_info "Step 4/4: Generating documentation..."
        dbt docs generate
        
        print_success "Complete pipeline finished!"
        ;;
    
    "incremental")
        print_info "Running incremental load..."
        dbt run
        print_success "Incremental load completed!"
        ;;
    
    "debug")
        print_info "Running dbt debug..."
        dbt debug
        ;;
    
    "clean")
        print_info "Cleaning dbt artifacts..."
        dbt clean
        print_success "Cleaned!"
        ;;
    
    *)
        echo "Usage: ./run_dbt.sh [command]"
        echo ""
        echo "Available commands:"
        echo "  deps           - Install dbt dependencies"
        echo "  full-refresh   - Run all models with full refresh"
        echo "  staging        - Run only staging models"
        echo "  intermediate   - Run only intermediate models"
        echo "  gold           - Run only gold models"
        echo "  dims           - Run only dimension models"
        echo "  facts          - Run only fact models"
        echo "  test           - Run all tests"
        echo "  docs           - Generate and serve documentation"
        echo "  all            - Run complete pipeline (deps + run + test + docs)"
        echo "  incremental    - Run incremental load"
        echo "  debug          - Run dbt debug"
        echo "  clean          - Clean dbt artifacts"
        echo ""
        echo "Examples:"
        echo "  ./run_dbt.sh deps"
        echo "  ./run_dbt.sh full-refresh"
        echo "  ./run_dbt.sh all"
        ;;
esac
