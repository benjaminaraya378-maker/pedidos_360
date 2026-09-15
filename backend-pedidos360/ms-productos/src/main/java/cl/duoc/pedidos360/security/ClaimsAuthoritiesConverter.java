package cl.duoc.pedidos360.security;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Traduce los claims del JWT a authorities de Spring Security:
 * <pre>
 *   roles: ["Admin"]                       → ROLE_ADMIN            (usable con hasRole('ADMIN'))
 *   scp:   "Pedidos360.Read Pedidos360.Write" → SCOPE_Pedidos360.Read, SCOPE_Pedidos360.Write
 * </pre>
 */
public class ClaimsAuthoritiesConverter implements Converter<Jwt, Collection<GrantedAuthority>> {

  private final String rolesClaim;

  public ClaimsAuthoritiesConverter(String rolesClaim) {
    this.rolesClaim = rolesClaim;
  }

  @Override
  public Collection<GrantedAuthority> convert(Jwt jwt) {
    Set<GrantedAuthority> authorities = new LinkedHashSet<>();
    for (String role : JwtUtils.roles(jwt, rolesClaim)) {
      authorities.add(new SimpleGrantedAuthority("ROLE_" + role.toUpperCase()));
    }
    for (String scope : JwtUtils.scopes(jwt)) {
      authorities.add(new SimpleGrantedAuthority("SCOPE_" + scope));
    }
    return authorities;
  }
}
