package com.yieldbook.mortgage.spring.dao;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.yieldbook.mortgage.spring.bean.Servicer;

@Component("servicersDao")
public class ServicersDAO {
	
	private NamedParameterJdbcTemplate npjdbc;
	private JdbcTemplate jdbc;
	
	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.npjdbc = new NamedParameterJdbcTemplate(dataSource);
		this.jdbc = new JdbcTemplate(dataSource);
	}

	public List<Servicer> getServicers() {
		
		return npjdbc.query("select * from prd2..servicer", new RowMapper<Servicer>() {

			public Servicer mapRow(ResultSet rs, int rowNum) throws SQLException {
				Servicer servicer = new Servicer();
				
				servicer.setServicerId(rs.getInt("servicer_id"));
				servicer.setServicer(rs.getString("servicer"));
				servicer.setLastChgDate(rs.getTimestamp("last_chg_date"));
				
				return servicer;
			}
			
		});
	}
	
	public boolean delete(int id) {
		MapSqlParameterSource params = new MapSqlParameterSource("servicer_id", id);
		
		return npjdbc.update("delete from prd2..servicer where servicer_id=:servicer_id", params) == 1;
	}

	public Servicer getServicerById(int id) {

		MapSqlParameterSource params = new MapSqlParameterSource();
		params.addValue("servicer_id", id);

		return npjdbc.queryForObject("select * from prd2..servicer where servicer_id=:servicer_id", params,
				new RowMapper<Servicer>() {

					public Servicer mapRow(ResultSet rs, int rowNum)
							throws SQLException {
						Servicer servicer = new Servicer();

						servicer.setServicerId(rs.getInt("servicer_id"));
						servicer.setServicer(rs.getString("servicer"));
						servicer.setLastChgDate(rs.getTimestamp("last_chg_date"));

						return servicer;
					}

				});
	}
	

	@Cacheable("servicers")
	public Servicer getServicerByName(String servicer){
		MapSqlParameterSource params = new MapSqlParameterSource();
		params.addValue("servicer", servicer);

		return npjdbc.queryForObject("select * from prd2..servicer where servicer=:servicer", params,
				new RowMapper<Servicer>() {

					public Servicer mapRow(ResultSet rs, int rowNum)
							throws SQLException {
						Servicer servicer = new Servicer();

						servicer.setServicerId(rs.getInt("servicer_id"));
						servicer.setServicer(rs.getString("servicer"));
						servicer.setLastChgDate(rs.getTimestamp("last_chg_date"));

						return servicer;
					}

				});		
	}
	
	public boolean create(Servicer servicer) {
		String sql = "select max(servicer_id) from prd2..servicer";
		int maxServiserId = jdbc.queryForInt(sql);
		MapSqlParameterSource params = new MapSqlParameterSource();
		params.addValue("servicer_id",maxServiserId+1);
		params.addValue("servicer", servicer.getServicer());
		params.addValue("last_chg_date", servicer.getLastChgDate());
		return npjdbc
				.update("insert into prd2..servicer(servicer_id, servicer, last_chg_date)values(:servicer_id,:servicer,:last_chg_date)",
						params) == 1;

	}
	
	public boolean update(Servicer servicer) {
		BeanPropertySqlParameterSource params = new BeanPropertySqlParameterSource(servicer);
		
		return jdbc.update("update prd2..servicer set servicer=:servicer, last_chg_date=:last_chg_date where servicer_id=:servicerId", params) == 1;
	}	
}
