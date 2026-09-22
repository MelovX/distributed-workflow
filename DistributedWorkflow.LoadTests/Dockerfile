FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["DistributedWorkflow.LoadTests/DistributedWorkflow.LoadTests.csproj", "DistributedWorkflow.LoadTests/"]
RUN dotnet restore "DistributedWorkflow.LoadTests/DistributedWorkflow.LoadTests.csproj"

COPY . .
WORKDIR /src/DistributedWorkflow.LoadTests
RUN dotnet publish "DistributedWorkflow.LoadTests.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DistributedWorkflow.LoadTests.dll"]
