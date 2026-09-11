#!/usr/bin/env python3
"""
Deploy Cocos application with Docker Compose.
Manages bio-service, mock bio-service, and web service stacks.

Usage:
    python deploy.py [--no-build] [--mock] [--down]

Options:
    --no-build  Skip rebuilding images
    --mock      Use the mock bio-service instead of the real bio-service
    --down      Stop and remove all Cocos containers
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


class DockerDeployer:
    """Manages Docker deployment for Cocos services."""

    NETWORK_NAME = "bio-network"

    BIO_SERVICE_PROJECT = "bio-service"
    MOCK_BIO_SERVICE_PROJECT = "mock-bio-service"
    WEB_SERVICE_PROJECT = "bio-web"

    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)

        self.bio_service_dir = (
            self.project_root
            / "docker-servicios-bio"
            / "docker-servicios-bio"
        )

        self.web_service_dir = (
            self.project_root
            / "cocos"
            / "docker"
        )

        self.mock_bio_service_dir = (
            self.project_root
            / "mock-bio-service"
        )

    def validate_setup(self, mock: bool = False) -> bool:
        """Validate project structure and prerequisites."""
        print("Validating setup...")

        if not shutil.which("docker"):
            print("Docker is not installed or not in PATH")
            return False

        dirs = {
            "Web Service": self.web_service_dir,
        }

        if mock:
            dirs["Mock Bio Service"] = self.mock_bio_service_dir
        else:
            dirs["Bio Service"] = self.bio_service_dir

        for name, path in dirs.items():
            docker_compose_file = path / "docker-compose.yml"

            if not docker_compose_file.exists():
                print(
                    f"{name} docker-compose.yml not found at "
                    f"{docker_compose_file}"
                )
                return False

            print(f"{name} found at {path}")

        return True

    def run_docker(self, cmd: list[str]) -> int:
        """Execute a Docker command."""
        print(f"Execute {' '.join(cmd)}")

        try:
            return subprocess.run(
                cmd,
                check=False,
            ).returncode
        except FileNotFoundError as e:
            print(f"Command failed: {e}")
            return 1

    def ensure_network(self) -> bool:
        """Create Docker network if it doesn't exist."""
        print(f"\nChecking network '{self.NETWORK_NAME}'...")

        result = subprocess.run(
            [
                "docker",
                "network",
                "inspect",
                self.NETWORK_NAME,
            ],
            capture_output=True,
            check=False,
        )

        if result.returncode == 0:
            print(
                f"Network '{self.NETWORK_NAME}' already exists"
            )
            return True

        print(
            f"Creating Docker network '{self.NETWORK_NAME}'..."
        )

        if self.run_docker(
            [
                "docker",
                "network",
                "create",
                self.NETWORK_NAME,
            ]
        ) != 0:
            print(
                f"Failed to create network "
                f"'{self.NETWORK_NAME}'"
            )
            return False

        print("Network created successfully")
        return True

    def deploy_service(
        self,
        name: str,
        compose_file: Path,
        project: str,
        build: bool = True,
    ) -> bool:
        """Deploy a Docker Compose service."""
        print(f"\nDeploying {name}...")

        if not compose_file.exists():
            print(
                f"docker-compose.yml not found at "
                f"{compose_file}"
            )
            return False

        cmd = [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "-p",
            project,
            "up",
            "-d",
        ]

        if build:
            cmd.append("--build")

        if self.run_docker(cmd) != 0:
            print(f"Failed to deploy {name}")
            return False

        print(f"{name} deployed successfully")
        return True

    def stop_project(
        self,
        project: str,
        compose_file: Path,
    ) -> bool:
        """
        Stop and remove a Docker Compose project.

        Returns True if the project was stopped successfully or
        if its compose file does not exist.
        """
        if not compose_file.exists():
            print(
                f"Skipping {project}: "
                f"compose file not found"
            )
            return True

        print(f"Stopping {project}...")

        result = self.run_docker(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "-p",
                project,
                "down",
            ]
        )

        if result != 0:
            print(
                f"Warning: Failed to stop {project}"
            )
            return False

        print(f"{project} stopped")
        return True

    def stop_bio_service(self, mock: bool) -> bool:
        """
        Stop the bio-service that should not be running.

        When using the mock, stop the real bio-service.
        When using the real service, stop the mock.
        """
        if mock:
            print(
                "\nCleaning up real bio-service "
                "before starting mock..."
            )

            return self.stop_project(
                self.BIO_SERVICE_PROJECT,
                self.bio_service_dir
                / "docker-compose.yml",
            )

        print(
            "\nCleaning up mock bio-service "
            "before starting real service..."
        )

        return self.stop_project(
            self.MOCK_BIO_SERVICE_PROJECT,
            self.mock_bio_service_dir
            / "docker-compose.yml",
        )

    def stop_services(self) -> bool:
        """Stop and remove all Cocos services."""
        print("\nStopping services...")

        services = [
            (
                self.BIO_SERVICE_PROJECT,
                self.bio_service_dir
                / "docker-compose.yml",
            ),
            (
                self.MOCK_BIO_SERVICE_PROJECT,
                self.mock_bio_service_dir
                / "docker-compose.yml",
            ),
            (
                self.WEB_SERVICE_PROJECT,
                self.web_service_dir
                / "docker-compose.yml",
            ),
        ]

        all_ok = True

        for project, compose_file in services:
            if not self.stop_project(
                project,
                compose_file,
            ):
                all_ok = False

        return all_ok

    def deploy(
        self,
        build: bool = True,
        mock: bool = False,
    ) -> bool:
        """Deploy Cocos services."""

        if not self.validate_setup(mock=mock):
            return False

        if not self.ensure_network():
            return False

        # -----------------------------------------------------
        # Clean up the opposite bio-service
        # -----------------------------------------------------

        if not self.stop_bio_service(mock=mock):
            print(
                "Failed to clean up the previous "
                "bio-service"
            )
            return False

        # -----------------------------------------------------
        # Deploy selected bio-service
        # -----------------------------------------------------

        if mock:
            print("\nUsing MOCK bio-service")

            if not self.deploy_service(
                "Mock Bio Service",
                self.mock_bio_service_dir
                / "docker-compose.yml",
                self.MOCK_BIO_SERVICE_PROJECT,
                build=build,
            ):
                return False

        else:
            print("\nUsing REAL bio-service")

            if not self.deploy_service(
                "Bio Service",
                self.bio_service_dir
                / "docker-compose.yml",
                self.BIO_SERVICE_PROJECT,
                build=build,
            ):
                return False

        # -----------------------------------------------------
        # Deploy web service
        # -----------------------------------------------------

        if not self.deploy_service(
            "Web Service",
            self.web_service_dir
            / "docker-compose.yml",
            self.WEB_SERVICE_PROJECT,
            build=build,
        ):
            return False

        return True


def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description=(
            "Deploy Cocos application with Docker Compose"
        )
    )

    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip rebuilding images",
    )

    parser.add_argument(
        "--mock",
        action="store_true",
        help=(
            "Use the mock bio-service instead of "
            "the real bio-service"
        ),
    )

    parser.add_argument(
        "--down",
        action="store_true",
        help=(
            "Stop and remove all Cocos containers"
        ),
    )

    args = parser.parse_args()

    project_root = Path(__file__).parent
    deployer = DockerDeployer(project_root)

    print("Cocos Docker Deployment")
    print("=" * 60)

    # ---------------------------------------------------------
    # DOWN
    # ---------------------------------------------------------

    if args.down:
        success = deployer.stop_services()

        if success:
            print(
                "\nServices stopped successfully"
            )
        else:
            print(
                "\nSome services failed to stop"
            )

        return 0 if success else 1

    # ---------------------------------------------------------
    # DEPLOY
    # ---------------------------------------------------------

    success = deployer.deploy(
        build=not args.no_build,
        mock=args.mock,
    )

    if success:
        print("\n" + "=" * 60)
        print("Deployment completed successfully!")

        print(
            "\nServices are running on the "
            f"'{deployer.NETWORK_NAME}' network:"
        )

        if args.mock:
            print(
                "  • Mock Bio Service: "
                "http://localhost:8001"
            )
        else:
            print(
                "  • Bio Service API: "
                "http://localhost:8001"
            )

        print(
            "  • Web Service: "
            "http://localhost:8080"
        )

        print("\nUseful commands:")

        print(
            "  • Stop all: "
            "python deploy.py --down"
        )

        print(
            "  • Deploy real bio-service: "
            "python deploy.py"
        )

        print(
            "  • Deploy mock bio-service: "
            "python deploy.py --mock"
        )

        print(
            "  • Deploy without rebuilding: "
            "python deploy.py --no-build"
        )

        print(
            "  • Mock without rebuilding: "
            "python deploy.py --mock --no-build"
        )

        return 0

    print("\nDeployment failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())