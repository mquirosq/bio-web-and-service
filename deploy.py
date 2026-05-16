#!/usr/bin/env python3
"""
Deploy Cocos application with Docker Compose.
Manages bio-service and web service stacks.

Usage:
    python deploy.py [--build] [--down]

Options:
    --build     Force rebuild images (default: True)
    --down      Stop and remove containers instead of deploying
"""

import subprocess
import sys
import argparse
from pathlib import Path
import shutil


class DockerDeployer:
    """Manages Docker deployment for Cocos services."""
    
    NETWORK_NAME = "bio-network"
    BIO_SERVICE_PROJECT = "bio-service"
    WEB_SERVICE_PROJECT = "bio-web"
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.bio_service_dir = self.project_root / "docker-servicios-bio" / "docker-servicios-bio"
        self.web_service_dir = self.project_root / "cocos" / "docker"
    
    def validate_setup(self) -> bool:
        """Validate project structure and prerequisites."""
        print("Validating setup...")
        
        # Check Docker installation
        if not shutil.which("docker"):
            print("Docker is not installed or not in PATH")
            return False
        
        # Check directories
        dirs = {
            "Bio Service": self.bio_service_dir,
            "Web Service": self.web_service_dir,
        }
        
        for name, path in dirs.items():
            docker_compose_file = path / "docker-compose.yml"
            if not docker_compose_file.exists():
                print(f"{name} docker-compose.yml not found at {docker_compose_file}")
                return False
            print(f"{name} found at {path}")
        
        return True
    
    def run_docker(self, cmd: list) -> int:
        """Execute docker command."""
        print(f"Execute {' '.join(cmd)}")
        try:
            return subprocess.run(cmd, check=False).returncode
        except FileNotFoundError as e:
            print(f"Command failed: {e}")
            return 1
    
    def ensure_network(self) -> bool:
        """Create Docker network if it doesn't exist."""
        print(f"\n Checking network '{self.NETWORK_NAME}'...")
        
        # Check if network exists
        result = subprocess.run(
            ["docker", "network", "inspect", self.NETWORK_NAME],
            capture_output=True,
            check=False
        )
        
        if result.returncode == 0:
            print(f"Network '{self.NETWORK_NAME}' already exists")
            return True
        
        # Create network
        print(f"  Creating network '{self.NETWORK_NAME}'...")
        if self.run_docker(["docker", "network", "create", self.NETWORK_NAME]) != 0:
            print(f"Failed to create network '{self.NETWORK_NAME}'")
            return False
        
        print(f"Network created successfully")
        return True
    
    def deploy_service(self, name: str, compose_file: Path, project: str, build: bool = True) -> bool:
        """Deploy a docker-compose service."""
        print(f"\n Deploying {name}...")
        
        if not compose_file.exists():
            print(f"docker-compose.yml not found at {compose_file}")
            return False
        
        cmd = ["docker", "compose", "-f", str(compose_file), "-p", project, "up", "-d"]
        
        if build:
            cmd.append("--build")
        
        if self.run_docker(cmd) != 0:
            print(f"Failed to deploy {name}")
            return False
        
        print(f"{name} deployed successfully")
        return True
    
    def stop_services(self) -> bool:
        """Stop and remove all services."""
        print("\n Stopping services...")
        
        services = [
            (self.BIO_SERVICE_PROJECT, self.bio_service_dir / "docker-compose.yml"),
            (self.WEB_SERVICE_PROJECT, self.web_service_dir / "docker-compose.yml"),
        ]
        
        all_ok = True
        for project, compose_file in services:
            print(f" Stopping {project}...")
            if self.run_docker(["docker", "compose", "-f", str(compose_file), "-p", project, "down"]) != 0:
                print(f" Warning: Failed to stop {project}")
                all_ok = False
            else:
                print(f"{project} stopped")
        
        return all_ok
    
    def deploy(self, build: bool = True) -> bool:
        """Deploy all services."""
        if not self.validate_setup():
            return False
        
        if not self.ensure_network():
            return False
        
        # Deploy services in order
        if not self.deploy_service(
            "Bio Service",
            self.bio_service_dir / "docker-compose.yml",
            self.BIO_SERVICE_PROJECT,
            build=build
        ):
            return False
        
        if not self.deploy_service(
            "Web Service",
            self.web_service_dir / "docker-compose.yml",
            self.WEB_SERVICE_PROJECT,
            build=build
        ):
            return False
        
        return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Deploy Cocos application with Docker Compose"
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip rebuilding images"
    )
    parser.add_argument(
        "--down",
        action="store_true",
        help="Stop and remove containers instead of deploying"
    )
    
    args = parser.parse_args()
    
    project_root = Path(__file__).parent
    deployer = DockerDeployer(project_root)
    
    print("Cocos Docker Deployment")
    print("=" * 60)
    
    if args.down:
        # Stop services
        success = deployer.stop_services()
        if success:
            print("\n Services stopped successfully")
        else:
            print("\n Some services failed to stop")
        return 0 if success else 1
    else:
        # Deploy services
        success = deployer.deploy(build=not args.no_build)
        
        if success:
            print("\n" + "=" * 60)
            print("Deployment completed successfully!")
            print("\nServices are running on the 'bio-network' network:")
            print("  • Bio Service API: http://localhost:8001 (check docker logs)")
            print("  • Web Service: http://localhost:8080")
            print("\nUseful commands:")
            print("  • View logs: docker logs -f bio-service-{service}")
            print("  • Stop all: python deploy.py --down")
            print("  • Rebuild: python deploy.py --no-build")
            return 0
        else:
            print("\n Deployment failed")
            return 1


if __name__ == "__main__":
    sys.exit(main())
