FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# Intercept and destroy the expired Yarn key list before features install
RUN sudo rm -f /etc/apt/sources.list.d/yarn.list
RUN sudo apt-get update