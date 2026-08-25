FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["DistributedWorkflow.Worker/DistributedWorkflow.Worker.csproj", "DistributedWorkflow.Worker/"]
RUN dotnet restore "DistributedWorkflow.Worker/DistributedWorkflow.Worker.csproj"

COPY . .
WORKDIR "/src/DistributedWorkflow.Worker"
RUN dotnet publish "DistributedWorkflow.Worker.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

EXPOSE 8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DistributedWorkflow.Worker.dll"]
