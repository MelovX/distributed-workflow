FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["Interview.Playground.Worker/Interview.Playground.Worker.csproj", "Interview.Playground.Worker/"]
RUN dotnet restore "Interview.Playground.Worker/Interview.Playground.Worker.csproj"

COPY . .
WORKDIR "/src/Interview.Playground.Worker"
RUN dotnet publish "Interview.Playground.Worker.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Interview.Playground.Worker.dll"]