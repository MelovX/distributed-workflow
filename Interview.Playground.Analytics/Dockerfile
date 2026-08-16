FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["Interview.Playground.Analytics/Interview.Playground.Analytics.csproj", "Interview.Playground.Analytics/"]
RUN dotnet restore "Interview.Playground.Analytics/Interview.Playground.Analytics.csproj"

COPY . .
WORKDIR "/src/Interview.Playground.Analytics"
RUN dotnet publish "Interview.Playground.Analytics.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Interview.Playground.Analytics.dll"]