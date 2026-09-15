package cl.duoc.pedidos360.pedidos.model;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "pedidos")
@Getter
@Setter
@NoArgsConstructor
public class Pedido {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  /** Claim "oid" del token: identifica al cliente dentro del tenant. */
  @Column(name = "cliente_oid", nullable = false, length = 64)
  private String clienteOid;

  @Column(name = "cliente_email", length = 160)
  private String clienteEmail;

  @Column(name = "cliente_nombre", length = 160)
  private String clienteNombre;

  @Column(nullable = false)
  private Instant fecha = Instant.now();

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 20)
  private EstadoPedido estado = EstadoPedido.CREADO;

  @Column(nullable = false, precision = 14, scale = 2)
  private BigDecimal total = BigDecimal.ZERO;

  @Column(length = 300)
  private String observacion;

  @OneToMany(mappedBy = "pedido", cascade = CascadeType.ALL, orphanRemoval = true)
  private List<PedidoItem> items = new ArrayList<>();

  public void agregarItem(PedidoItem item) {
    item.setPedido(this);
    items.add(item);
  }
}
