using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DistributedWorkflow.Api.Migrations
{
    /// <inheritdoc />
    public partial class AddUnpublishedOutboxIndex : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessages_Unpublished_CreatedAt_Id",
                table: "OutboxMessages",
                columns: new[] { "CreatedAt", "Id" },
                filter: "\"PublishedAt\" IS NULL")
                .Annotation("Npgsql:CreatedConcurrently", true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_OutboxMessages_Unpublished_CreatedAt_Id",
                table: "OutboxMessages");
        }
    }
}
