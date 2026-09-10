FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["DistributedWorkflow.Migrations/DistributedWorkflow.Migrations.csproj", "DistributedWorkflow.Migrations/"]
COPY ["DistributedWorkflow.Api/DistributedWorkflow.Api.csproj", "DistributedWorkflow.Api/"]
COPY ["DistributedWorkflow.Worker/DistributedWorkflow.Worker.csproj", "DistributedWorkflow.Worker/"]
RUN dotnet restore "DistributedWorkflow.Migrations/DistributedWorkflow.Migrations.csproj"

COPY . .
WORKDIR "/src/DistributedWorkflow.Migrations"
RUN dotnet publish "DistributedWorkflow.Migrations.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DistributedWorkflow.Migrations.dll"]