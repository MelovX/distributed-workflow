using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DistributedWorkflow.Api.Migrations
{
    /// <inheritdoc />
    public partial class AddRegistrationIdempotencyKey : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "IdempotencyKey",
                table: "RegistrationOperations",
                type: "text",
                nullable: false,
                defaultValue: "");

            migrationBuilder.CreateIndex(
                name: "IX_RegistrationOperations_IdempotencyKey",
                table: "RegistrationOperations",
                column: "IdempotencyKey",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_RegistrationOperations_IdempotencyKey",
                table: "RegistrationOperations");

            migrationBuilder.DropColumn(
                name: "IdempotencyKey",
                table: "RegistrationOperations");
        }
    }
}
