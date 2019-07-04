# OPC Publisher by AIT
# Debug HowTo with AIT Module Configurator with IoT Edge Simulator

## Build
1. Build Docker Image in Debug Mode
```
docker build -f .\docker\linux\amd64\Dockerfile.debug -t localhost:5000/opcpublisher:0.0.1-amd64 .
```
2. Push Image to local Registry (If you dont have a local registry read README of ModuleConfigurator first)
```
docker push localhost:5000/opcpublisher:0.0.1-amd64
```

## Debug

Use the following VS Code Commands

1. Build and Run IoT Edge Solution in Simulator (ModuleConfigurator)
2. If you have problems with startup you can edit Dockerfile.debug to ENTRYPOINT ["dotnet", "/app/opcpublisher.dll", "wfd"] (wfd stands for wait for debugger) then the module is waiting that you connect your debugger otherwise use the existing Dockerfile
2. Choose OPCPublisher Remote Debug (.NET Core) from launch.json