using DapperLicenta.Datas;
using DapperLicenta.Repos;
using Microsoft.AspNetCore.Mvc;

namespace LicentaEnityFrameworkAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class LicentaController : ControllerBase
    {
        private readonly ILicentaRepo licentaRepo;

        public LicentaController(ILicentaRepo licentaRepo)
        {
            this.licentaRepo = licentaRepo;
        }

        [HttpPost()]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [Route("insert-team")]
        public IActionResult InsertTeam([FromBody] TeamData teamData)
        {
            licentaRepo.InsertTeam(teamData);

            return Created("/", "Created");
        }

        [HttpPut()]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [Route("update-team")]
        public IActionResult UpdateTeam([FromBody] TeamData teamData)
        {
            licentaRepo.UpdateTeam(teamData);

            return Ok("Updated");
        }

        [HttpDelete()]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [Route("delete-player/{position}/{age}")]
        public IActionResult DeleteTeam(string position, long age)
        {
            licentaRepo.DeletePlayer(position, age);

            return Ok("Deleted");
        }

        [HttpGet()]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [Route("player-by-position/{position}")]
        public int GetPlayerAndTeamByPosition(string position)
        {
            return licentaRepo.GetPlayerAndTeamByPosition(position);
        }

        [HttpGet()]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [Route("players")]
        public int GetPlayers()
        {
            return licentaRepo.GetAllPlayers();
        }

        [HttpGet()]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [Route("teams-with-at-least-points/{points}")]
        public int GetTeamsByPoints(long points)
        {
            return licentaRepo.GetTeamsByPoints(points);
        }

    }
}